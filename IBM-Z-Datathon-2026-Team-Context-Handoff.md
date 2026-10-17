# IBM Z Datathon 2026 — Team Context Handoff

Oct 17, 2026

## How to use this

Paste this whole document into a new Claude chat as your first message, then say what you're working on. Everything Claude needs to be useful on this project is below — the event, the rubric, the architecture, the data, the platform constraints, and the things we still don't know.

If you're the human reading it: you can skip to the section you need. The architecture and the rubric are the two that matter most for making decisions on your own.

One thing this document deliberately does not contain: who is doing what. That gets assigned separately.

## The event

IBM Z Datathon 2026, run by the Shooting Stars Foundation with IBM. A 24-hour global build, themed "AI for Good." We are a four-person team in the Americas region, competing in the Real-Time AI for Critical Decisions track (tagline: Detect. Decide. Respond.).

There is no assigned problem statement. Teams pick their own problem under a theme. Ours is card fraud.

Schedule, all Eastern:

| When | What |
| --- | --- |
| Oct 17, 9–10 AM | Americas hybrid kick-off |
| Oct 17, 9 AM–5 PM | Day 1 in person at NYU (and UTD for the other Americas hub) |
| Oct 17, 5 PM – Oct 18, 10 AM | Virtual continuation overnight |
| Oct 18, 10:30 AM | Americas closing ceremony |
| Oct 28, 10–11:30 AM | Career panel and winner announcements |

Prizes come from a $50,000+ pool, split into Online Regional Champions (top 3 each for Americas, EMEA, APAC) and In-Person Hub Champions (top 3 at each of 8 hubs). Those are separate prize lines. There are also special awards for Most Innovative AI Theme and Most Creative AI Solution, which are judged on a different rubric and explicitly do not go to the highest-scoring teams.

Support during the build runs through Discord — #announcements, #general, #help, and #ibm-z-help, which only opens on the day. IBM subject-matter experts are on the helpdesk as mentors for the full 24 hours. Use them; past winners did.

## What we're building

**Problem.** Most card fraud is caught after settlement, so the money is already gone and low-balance cardholders wait weeks on provisional credit.

**Solution.** A streaming pipeline on IBM LinuxONE that scores every transaction at ingest with a model ensemble and writes a plain-language reason for each alert.

**The edge is instrumentation, not accuracy.** Measured throughput and p95 latency, a dead-letter queue that survives bad input, and explanations that let analysts triage in seconds.

That third line is the part people skip, so it's worth stating directly. We are not trying to build the most accurate fraud classifier in the room. Detection is a solved-enough problem and every team with a Kaggle notebook can claim 97% accuracy. What almost nobody does is build a system — something that ingests continuously, survives malformed input, reports what it actually achieved under load, and explains its own output. The bottleneck in real fraud operations is not detection, it's triage: an analyst staring at a queue of scores with no reasoning attached. That's the gap we're aiming at.

## Architecture

Five stages, each containerized and running on the LinuxONE Community Cloud. Written out as a flow:

`producer → validation → ensemble scoring → explanation layer → PostgreSQL → dashboard`

with validation also branching sideways into a dead-letter queue, and the dashboard reading from Postgres.

**1. Event producer.** Replays the Sparkov card-transaction dataset as a live event stream at a configurable rate, in true chronological order. Not a batch job reading a CSV — the point is that events arrive one at a time, as they would in production, and the rate is a dial we can turn up to find where the system breaks.

**2. Schema validation, with a dead-letter queue.** Every incoming event is schema-checked. Malformed records are routed to a dead-letter queue instead of crashing the consumer. This is a branch off the happy path, not a stage in it — that's the whole point. A bad record exits the pipeline and its count surfaces on the dashboard as a number we can point at during the demo. It takes about ten seconds to show and it is the clearest single piece of engineering judgment in the build.

**3. Ensemble scoring.** A multi-model ensemble assigns a fraud probability to each event. Classical models (XGBoost, random forest, logistic regression) are the right call here — they're what past winners used, they train fast, and they're prebuilt for this architecture. Hard constraint: the scoring call has to be fast enough that the latency number isn't embarrassing. If the ensemble takes 400ms per event, the whole latency story dies.

**4. Explanation layer.** A generative model turns the ensemble's score into one plain-language sentence a human can act on. This sits *after* scoring, not inside it, so a failure here can't take down scoring. "High fraud probability" is a worthless explanation. "Card used in Ohio 40 minutes after a Nevada purchase, merchant category unusual for this cardholder" is the thing that scores points. The planned path is calling IBM watsonx Assistant by API from the VM rather than running a generative model on the mainframe architecture ourselves.

**5. Storage and serving.** Scored events land in PostgreSQL alongside SQL aggregate views. A live dashboard reads from it and shows four things: the alert feed, sustained events per second, p50/p95 end-to-end latency, and dead-letter volume.

**Why the split matters.** Stages 3 and 4 together are what satisfies the two mandatory prerequisite labs — multi-model predictive AI lives in stage 3, predictive-plus-generative spans stages 3 and 4. Keeping them as separate containers is both the right engineering call and the thing that makes the lab mapping obvious to a judge.

**Contracts come before code.** The event JSON schema, the scored-event schema and the Postgres table shape should be published to the whole team before anyone writes anything, so every stage can be built against stubs instead of waiting on the stage upstream.

## The data

**Primary: Sparkov "Credit Card Transactions Fraud Detection"** (Kaggle, user kartik2112). Roughly 1.85M simulated transactions, January 2019 to December 2020, 1,000 cardholders across 800 merchants, fraud under 1%.

We chose it over the better-known ULB set for two reasons. It keeps real field names — merchant, category, amount, timestamp, location — which we need both to replay events in true chronological order and to generate explanations that say anything meaningful. And being synthetic, it keeps real cardholder PII off a shared cloud instance, which is a point worth making out loud to a judge.

**Secondary: the ULB European cardholder set.** 284,807 real transactions, 0.17% fraud. Used as a held-out generalization check against a genuinely different distribution. It is PCA-anonymized, so the explanation layer cannot produce meaningful text on it. Say that plainly if asked rather than implying the whole system runs on both. Cut this first if time runs short.

**Supporting: FTC Consumer Sentinel and the Nilson Report.** Public fraud-loss statistics, used to size the problem in the video. Not training data. Verify any specific dollar figure before it goes on screen.

**Two numbers not to misquote.** The \~1.85M is the full set: 1,296,675 in the training file plus roughly 555K in the test file. Quote whichever we actually replay — if it's only the training file, say 1.3M. And if anyone finds a 5.7% fraud rate quoted somewhere, don't use it; that's a resampled benchmark variant, not the raw Kaggle data, which runs near 0.5%.

## The platform

**What IBM Z is.** IBM's mainframe line — machines built for enormous volumes of transactional work with very high availability and encryption in the data path. What makes it relevant to us is the Telum processor's on-chip AI accelerator, designed to run model inference *during* a transaction rather than in a batch job afterward. That is exactly the gap our pitch opens with, which is why the platform story writes itself.

**LinuxONE** is the same hardware running only Linux. No z/OS, no COBOL, no green screens. The **LinuxONE Community Cloud (L1CC)** is free VMs on that hardware, and that is what we actually get. No documented past winner touched z/OS, Db2 or Zowe, so nobody needs to learn mainframe tooling.

**The catch: s390x and big-endian.** L1CC runs on the s390x architecture, which is big-endian. Not every container image or Python wheel is built for it. Two past winning teams hit big-endian PyTorch problems and had to work around them mid-event. Anything we pull has to be checked for an s390x build before we commit to it.

**What's already solved for us.** IBM publishes Fast Start Guides for L1CC, and three matter:

- The **AI/ML on Jupyter Notebooks** guide ships a container with Keras, TensorFlow, PyTorch, XGBoost and scikit-learn already built for s390x. That's the whole ensemble, prebuilt, with the big-endian trap handled inside the image. Start from this rather than pip-installing onto a bare VM.
- The **PostgreSQL on Docker** guide matches how we want to containerize everything anyway.
- The **watsonx Assistant** guide covers calling it by API from a Python script on the VM. This is what turns the explanation layer from "get a generative model running on s390x" into an API call.

**What is not covered anywhere.** There is no Fast Start Guide for Kafka or Redpanda. Streaming transport is our single open architecture risk. If neither has a usable s390x image, a plain Python producer writing to a queue we control is a legitimate fallback and costs us nothing in the rubric. Do not let this threaten a clean demo.

**Two mandatory prerequisite labs** must be completed before building our own solution: (1) Predictive AI hybrid scenarios, meaning multi-model, and (2) Predictive AI plus Generative AI. These are not optional and nothing in the tooling forces us to do them, which is exactly how teams forget.

## How we're judged

Four criteria, 10 points each, 40 total. This is from the official Participant Guidebook and supersedes any 100-point rubric found online.

| Criterion | What it rewards | Where we stand |
| --- | --- | --- |
| Value to the Community & Theme Alignment | A real problem, a named beneficiary, the theme visible in idea and execution | Strong, if we name a concrete victim rather than "banks" |
| Novelty & Idea Originality | Top band is breakthrough with strong end-to-end components; the 4–6 band is literally "relies on common or predictable ideas" | **Our exposed flank** — see below |
| Technical Design & Use of LinuxONE | Architecture strength, AI/ML genuinely running on LinuxONE, containerization and compute evidenced by screenshots | Strong, and the easiest points to lose by forgetting screenshots |
| Execution & Delivery / Pitch | Clarity, professionalism, a demo included, staying inside the video time limit | Entirely in our control, and the one a tired team drops |

**The Novelty problem, stated honestly.** Fraud detection is the guidebook's own first suggested idea for this track. A plain fraud classifier is the definition of "common or predictable" and caps us at 4–6 on that criterion. The explanation layer is our answer: it moves us off "another fraud classifier," it targets triage rather than detection, and it happens to satisfy the second prerequisite lab. The measured throughput and latency work is the other half of the answer.

**Execution is not the leftover job.** It is 10 of 40 points, and the video is the only artifact a judge actually experiences. A flawless pipeline described badly scores worse than a mediocre one demoed well.

## What past winners did

We researched the 2023–2025 editions before committing to this plan. The record is thin — the organizers publish team names and little else, so nearly everything known comes from about nine blog posts winners wrote themselves afterward. Treat it as directional, not definitive.

What the documented winners had in common:

- **Modest classical ML, actually deployed.** Random forest, XGBoost, logistic regression. Not novel architectures. What distinguished them was that the model genuinely ran on the LinuxONE Community Cloud and was shown doing so.
- **A quantified harm in the opening line.** Every one of them opened with a specific number attached to a specific victim, not a general appeal.
- **A working demo**, not slides.
- **A clear reason why IBM Z mattered** for their workload specifically.
- **None used z/OS, COBOL, Db2 or watsonx.**

Fraud is a proven winning domain here, not a handicap — the only 2024 project that publicly calls itself first place was an electricity-theft fraud detector, and another 2024 winning team built e-commerce transaction fraud detection.

**The open lane.** No documented winner in three years reported throughput, tail latency or any load testing. They reported accuracy. Our measured numbers are something nobody in the field's history has brought, which is why the instrumentation work is strategically worth as much as the modeling work.

**On our odds.** Documented winners came from Indian host campuses, London and South Africa. The 2026 format adds an Americas bracket plus in-person hub prizes at NYU and Dallas, each awarding a top three. The field we're actually competing against is much smaller than the headline registration numbers suggest.

## Hard rules

These are from the official guidebook. Breaking one can disqualify us.

- **No continuation hacks.** All code is written from scratch inside the Oct 17–18 window. Do not bring in a repo you started last week.
- **Pre-event work that was allowed:** research, dataset exploration, abstract drafting, architecture planning. All of that is done and is what this document records.
- **Reusing prior personal projects or commercial codebases as the core submission is disallowed.** Open-source libraries, public APIs and IBM-provided tools are fine, with links.
- **Projects must be original to the team.** Taking code or ideas from other participants is prohibited.
- **Every open-source resource we use needs a link in the submission.** Including the dataset license.
- **One project submission per team per track.**

## Submission, and what we still don't know

**What has to be handed in:**

- A video of **no more than 3 minutes**, covering the problem, the data supporting the problem, the solution, and above all how it leverages IBM Z — showing the tech stack, the LinuxONE instance, and the model on screen.
- Code: GitHub link or zip, plus links to every open-source resource used.
- A project title in the format `[Team Name_IBM Z Datathon 2026 Project]`.
- Optional but recommended: a design flowchart and a working prototype.
- Submitted through hack2skill.com/event/datathon-2026.

The 300–500 word abstract was already submitted on October 3. It committed us to the five-stage architecture, the Sparkov dataset, and the claim that we will report measured throughput and tail latency and publish the repository. We should deliver on all three.

**Open items, in rough priority:**

1. **The two prerequisite labs.** Mandatory. Unresolved: whether "mandatory" means per person or per team. If per person, that's four mornings. Ask in Discord #help early — it changes the whole schedule.
2. **The event code.** It was listed as TBD in the guidebook, it goes in our project description, and it comes through Discord #announcements.
3. **The Sparkov license.** Never confirmed — one source listed CC0 and MIT/Apache in consecutive lines. The submission requires properly licensed resources with links. Five-minute job, easy to forget at hour twenty.
4. **s390x image availability** for whatever handles streaming transport.
5. **The video deadline.** The guidebook lists Oct 23 midnight for the feedback survey and video, but closing is 10:30 AM on Oct 18. Don't bank on the extra days without confirming. Plan to freeze code at 7 AM Sunday either way.
6. **Team creation on Hack2skill**, which is a step separate from individual registration.
