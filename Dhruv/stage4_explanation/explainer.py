"""
explainer.py — stage 4. A scored event goes in, and the same scored event comes out
with its `explanation` field filled (scored_event.schema.json).

    from explainer import Explainer
    ex = Explainer(backend="template")        # or "watsonx"
    explained = ex.explain(scored_event)

What it writes in `explanation`:
    ALLOW          -> {"status": "skipped", ...}            no sentence needed
    REVIEW / BLOCK -> {"status": "ok", "text": "BLOCK: $24.84 at ... Why: ...", ...}
    AI call broke  -> {"status": "failed" or "timeout", "error": "...",
                       "text": <template sentence>, "model": "template-fallback"}
                      so an analyst never gets an alert with nothing to read.

Two ways to write the sentence ("backends"):
  - template: fills a fixed sentence from stage 3's reasons. Instant, never fails,
    no internet. Good default and the safety net.
  - watsonx:  asks an IBM Granite model on watsonx.ai to write the sentence from the
    same facts. Needs these environment variables (never put the key in code):
        WATSONX_APIKEY, WATSONX_PROJECT_ID,
        WATSONX_URL (default https://us-south.ml.cloud.ibm.com),
        WATSONX_MODEL_ID (default ibm/granite-3-8b-instruct)

Rules this file keeps:
  - It never reads event["label"] (the true answer) and never sends it anywhere.
  - It never sends card_id or card_last4 to the AI service.
  - It never changes the original event, only the `explanation` field.
"""

import json
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

TIMEOUT_MS = 800  # hard budget from the schema

# Sparkov category codes -> words an analyst would use.
CATEGORY_LABEL = {
    "entertainment": "entertainment", "food_dining": "food & dining",
    "gas_transport": "gas & transport", "grocery_net": "online grocery",
    "grocery_pos": "in-store grocery", "health_fitness": "health & fitness",
    "home": "home", "kids_pets": "kids & pets", "misc_net": "misc online",
    "misc_pos": "misc in-store", "personal_care": "personal care",
    "shopping_net": "online shopping", "shopping_pos": "in-store shopping",
    "travel": "travel",
}


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def facts_from(scored):
    """The only things the sentence is allowed to use. No label, no card id."""
    ev, s = scored["event"], scored["scoring"]
    tx, ch = ev["transaction"], ev["cardholder"]
    merchant = tx["merchant"]
    if merchant.startswith("fraud_"):  # every Sparkov merchant has this prefix; it means nothing
        merchant = merchant[len("fraud_"):]
    return {
        "decision": s["decision"],
        "amount": tx["amount"],
        "merchant": merchant,
        "category": CATEGORY_LABEL.get(tx["category"], tx["category"].replace("_", " ")),
        "home": f"{ch.get('city', '?')}, {ch.get('state', '?')}",
        "reasons": [r["detail"] if isinstance(r, dict) else str(r)
                    for r in s.get("reasons", []) if (isinstance(r, str) or r.get("detail"))],
        "score": s["ensemble_score"],
        "threshold": s["threshold"],
    }


class TemplateBackend:
    name = "template"

    def generate(self, f, timeout_s=None):
        head = f"{f['decision']}: ${f['amount']:,.2f} at {f['merchant']} ({f['category']}), cardholder in {f['home']}."
        if f["reasons"]:
            why = " Why: " + "; ".join(f["reasons"]) + "."
        else:  # scored without reasons; still say something true
            why = f" Risk score {f['score']:.2f} is above the alert line of {f['threshold']:.2f}."
        return (head + why)[:512]


class WatsonxBackend:
    """IBM watsonx.ai text generation over plain HTTPS (Python standard library only)."""

    def __init__(self):
        self.apikey = os.environ.get("WATSONX_APIKEY")
        self.project_id = os.environ.get("WATSONX_PROJECT_ID")
        if not self.apikey or not self.project_id:
            raise RuntimeError("watsonx backend needs WATSONX_APIKEY and WATSONX_PROJECT_ID environment variables")
        self.url = os.environ.get("WATSONX_URL", "https://us-south.ml.cloud.ibm.com").rstrip("/")
        self.model_id = os.environ.get("WATSONX_MODEL_ID", "ibm/granite-3-8b-instruct")
        self.name = self.model_id[:64]
        self._token, self._token_expires = None, 0
        self._get_token()  # fetch at startup, so the first alert isn't slow

    def _get_token(self):
        """Trade the API key for a short-lived access token (IBM Cloud IAM). Reused ~1 hour."""
        if self._token and time.time() < self._token_expires - 300:
            return self._token
        body = urllib.parse.urlencode({"grant_type": "urn:ibm:params:oauth:grant-type:apikey",
                                       "apikey": self.apikey}).encode()
        req = urllib.request.Request("https://iam.cloud.ibm.com/identity/token", data=body,
                                     headers={"Content-Type": "application/x-www-form-urlencoded",
                                              "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.load(r)
        self._token, self._token_expires = data["access_token"], time.time() + data.get("expires_in", 3600)
        return self._token

    @staticmethod
    def prompt(f):
        reasons = "\n".join(f"- {r}" for r in f["reasons"]) or f"- risk score {f['score']:.2f}"
        return (
            "You help a bank fraud analyst triage alerts. Write ONE sentence (at most 35 words) "
            "saying why this card payment was flagged, using only the facts below. Do not invent "
            "facts. Do not mention models, scores or probabilities.\n\n"
            f"Decision: {f['decision']}\n"
            f"Payment: ${f['amount']:,.2f} at {f['merchant']} ({f['category']})\n"
            f"Cardholder home: {f['home']}\n"
            f"Top reasons, most important first:\n{reasons}\n\n"
            "Sentence:"
        )

    def generate(self, f, timeout_s):
        body = json.dumps({
            "model_id": self.model_id,
            "project_id": self.project_id,
            "input": self.prompt(f),
            "parameters": {"decoding_method": "greedy", "max_new_tokens": 70, "stop_sequences": ["\n\n"]},
        }).encode()
        req = urllib.request.Request(
            f"{self.url}/ml/v1/text/generation?version=2023-05-29", data=body,
            headers={"Authorization": f"Bearer {self._get_token()}",
                     "Content-Type": "application/json", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout_s) as r:
            data = json.load(r)
        text = data["results"][0]["generated_text"].strip().split("\n")[0].strip()
        if not text:
            raise ValueError("model returned an empty sentence")
        return text[:512]


class Explainer:
    def __init__(self, backend="template", timeout_ms=TIMEOUT_MS):
        self.fallback = TemplateBackend()
        self.backend = WatsonxBackend() if backend == "watsonx" else self.fallback
        self.timeout_s = timeout_ms / 1000

    def explain(self, scored):
        """Returns a copy of `scored` with `explanation` filled. Never raises for one bad event."""
        start = time.perf_counter()
        out = dict(scored)  # the event inside is shared, not changed

        def done(**fields):
            fields["generated_at"] = _now()
            fields["duration_ms"] = round((time.perf_counter() - start) * 1000, 3)
            out["explanation"] = fields
            return out

        try:
            f = facts_from(scored)
        except (KeyError, TypeError, AttributeError) as e:
            return done(status="failed", error=f"scored event missing data: {type(e).__name__}: {e}"[:256],
                        model=self.backend.name)

        if f["decision"] == "ALLOW":
            return done(status="skipped", model=self.backend.name)

        try:
            text = self.backend.generate(f, self.timeout_s)
            return done(status="ok", text=text, model=self.backend.name)
        except (socket.timeout, TimeoutError) as e:
            status, err = "timeout", f"no answer within {self.timeout_s * 1000:.0f} ms"
        except urllib.error.URLError as e:
            if isinstance(getattr(e, "reason", None), socket.timeout):
                status, err = "timeout", f"no answer within {self.timeout_s * 1000:.0f} ms"
            else:
                status, err = "failed", f"{type(e).__name__}: {e}"
        except Exception as e:  # anything else from the AI call; the row must still be written
            status, err = "failed", f"{type(e).__name__}: {e}"
        # Safety net: the analyst still gets the template sentence, and status shows the AI failed.
        return done(status=status, error=err[:256], text=self.fallback.generate(f), model="template-fallback")
