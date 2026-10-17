"""
explainer.py — stage 4. A scored event goes in, and the same scored event comes out
with its `explanation` field filled (scored_event.schema.json).

    from explainer import Explainer
    ex = Explainer(backend="template")        # or "watsonx" or "local"
    explained = ex.explain(scored_event)

What it writes in `explanation`:
    ALLOW          -> {"status": "skipped", ...}            no sentence needed
    REVIEW / BLOCK -> {"status": "ok", "text": "BLOCK: $24.84 at ... Why: ...", ...}
    AI call broke  -> {"status": "failed" or "timeout", "error": "...",
                       "text": <template sentence>, "model": "template-fallback"}
                      so an analyst never gets an alert with nothing to read.

Three ways to write the sentence ("backends"):
  - template: fills a fixed sentence from stage 3's reasons. Instant, never fails,
    no internet. Good default and the safety net.
  - watsonx:  asks an IBM Granite model on watsonx.ai to write the sentence from the
    same facts. Needs these environment variables (never put the key in code):
        WATSONX_APIKEY, WATSONX_PROJECT_ID,
        WATSONX_URL (default https://us-south.ml.cloud.ibm.com),
        WATSONX_MODEL_ID (default ibm/granite-3-8b-instruct)
  - local:    asks a Granite model WE run, with llama.cpp's `llama-server`, on the
    same machine (LinuxONE in the final setup). No API key, no internet, no
    payment data leaves the machine. Settings:
        LLAMA_URL (default http://127.0.0.1:8080)
        LLAMA_MODEL_NAME (optional; shown in the "model" field, else asked from the server)

Rules this file keeps:
  - It never reads event["label"] (the true answer) and never sends it anywhere.
  - It never sends card_id or card_last4 to the AI service.
  - It never changes the original event, only the `explanation` field.
"""

import json
import os
import re
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


def facts_text(f):
    """The facts block, exactly as the AI sees it. Also what check_numbers() checks against."""
    reasons = "\n".join(f"- {r}" for r in f["reasons"]) or f"- risk score {f['score']:.2f}"
    return (f"Decision: {f['decision']}\n"
            f"Payment: ${f['amount']:,.2f} at {f['merchant']} ({f['category']})\n"
            f"Cardholder home: {f['home']}\n"
            f"Top reasons, most important first:\n{reasons}")


def build_prompt(f):
    """Instructions + one worked example + this alert's facts. Small models copy an example's
    style closely, so the example shows exactly what we want: short, factual, numbers copied."""
    return (
        "You help a bank fraud analyst triage alerts. Write ONE short sentence (at most 25 words) "
        "saying why this card payment was flagged.\n"
        "Rules: use only the facts listed. Copy numbers exactly as written. Do not guess anything about "
        "the merchant, location or cardholder that is not listed. Do not mention models, scores or probabilities.\n\n"
        "Example facts:\n"
        "Decision: REVIEW\n"
        "Payment: $412.10 at Smith LLC (online shopping)\n"
        "Cardholder home: Austin, TX\n"
        "Top reasons, most important first:\n"
        "- amount $412.10 is 8.2x this card's usual $50\n"
        "- made at 23:00 (late night)\n"
        "Example sentence: $412.10 online purchase at 23:00, 8.2x this card's usual $50.\n\n"
        f"Facts:\n{facts_text(f)}\n"
        "Sentence:"
    )


_NUMBER = re.compile(r"\d[\d,]*(?:\.\d+)?")


def _numbers(text):
    return {float(n.replace(",", "")) for n in _NUMBER.findall(text)}


def check_numbers(sentence, f):
    """Every number the AI wrote must appear in the facts it was given (13x == 13.0x is fine).
    Catches the classic small-model mistake of '130x' for '13.0x'. Raises ValueError if not."""
    unknown = sorted(_numbers(sentence) - _numbers(facts_text(f)))
    if unknown:
        raise ValueError(f"AI sentence used numbers not in the facts: {', '.join(f'{n:g}' for n in unknown[:5])}")
    return sentence


def clean_sentence(text, cut_off=False):
    """Keep the first real line of what the model said, minus quotes or a 'Sentence:' echo.
    cut_off=True means the model hit its word limit mid-sentence: trim back to the last
    complete phrase so the analyst never reads half a word."""
    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
    if not lines:
        raise ValueError("model returned an empty sentence")
    line = lines[0]
    if line.lower().startswith("sentence:"):
        line = line[len("sentence:"):].strip()
    line = line.strip('"\'' ).strip()
    if cut_off and not line.endswith((".", "!", "?")):
        cut = max(line.rfind(", "), line.rfind("; "), line.rfind(" and "))
        if cut > len(line) * 0.4:  # only if that keeps most of the sentence
            line = line[:cut]
        line = line.rstrip(",;: ") + "."
    if not line:
        raise ValueError("model returned an empty sentence")
    return line[:512]


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

    prompt = staticmethod(build_prompt)  # kept so WatsonxBackend.prompt(f) still works

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
        res = data["results"][0]
        return check_numbers(clean_sentence(res["generated_text"], cut_off=res.get("stop_reason") == "max_tokens"), f)


class LocalBackend:
    """A Granite model we run ourselves with llama.cpp's `llama-server`.
    Uses its OpenAI-style chat endpoint, which applies Granite's chat format for us."""

    def __init__(self):
        self.url = os.environ.get("LLAMA_URL", "http://127.0.0.1:8080").rstrip("/")
        try:  # fail loudly at startup, not on the first alert
            with urllib.request.urlopen(f"{self.url}/health", timeout=5) as r:
                ok = json.load(r).get("status") == "ok"
        except (urllib.error.URLError, OSError, ValueError) as e:
            raise RuntimeError(f"local backend: no llama-server ready at {self.url} ({e}). "
                               "Start it first (see README).") from e
        if not ok:
            raise RuntimeError(f"local backend: llama-server at {self.url} is not ready yet")
        name = os.environ.get("LLAMA_MODEL_NAME")
        if not name:
            try:
                with urllib.request.urlopen(f"{self.url}/v1/models", timeout=5) as r:
                    name = os.path.basename(json.load(r)["data"][0]["id"])
            except Exception:
                name = "llama.cpp-local"
        self.name = name[:64]

    def generate(self, f, timeout_s):
        body = json.dumps({
            "messages": [{"role": "user", "content": build_prompt(f)}],
            "max_tokens": 60,   # ~45 words max; each token costs ~35 ms on the Mac
            "temperature": 0,   # same facts -> same sentence
        }).encode()
        req = urllib.request.Request(f"{self.url}/v1/chat/completions", data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout_s) as r:
            data = json.load(r)
        choice = data["choices"][0]
        return check_numbers(clean_sentence(choice["message"]["content"], cut_off=choice.get("finish_reason") == "length"), f)


class Explainer:
    def __init__(self, backend="template", timeout_ms=TIMEOUT_MS):
        self.fallback = TemplateBackend()
        if backend == "watsonx":
            self.backend = WatsonxBackend()
        elif backend == "local":
            self.backend = LocalBackend()
        elif backend == "template":
            self.backend = self.fallback
        else:
            raise ValueError(f"unknown backend {backend!r}: use template, watsonx or local")
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
