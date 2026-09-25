# Switchyard

A distributed, DAG-based workflow orchestrator built from first principles — a lightweight Airflow/Temporal-style system where the hard distributed-systems problems (exactly-once dispatch, worker failure detection, retries, crash recovery, compensating transactions) are implemented directly rather than hidden behind a framework.

This project exists to demonstrate real distributed-systems engineering: durable state, idempotent task execution, failure detection via heartbeats/lease expiry, and recovery under chaos testing — not just a CRUD app with a task list.

---

## 1. Problem Statement

Modern applications frequently need to run multi-step workflows where individual tasks depend on one another (e.g. "extract data → transform → validate → load → notify"). Running these reliably at scale is hard: tasks can fail mid-execution, workers can crash, network partitions can occur, and naive retry logic can cause duplicate side effects or lost work.

Existing orchestrators (Airflow, Temporal, Celery) solve this, but abstract the mechanism away. This project builds the mechanism itself.

## 2. What Switchyard Does

- Workflows are defined as a DAG of tasks with explicit dependencies
- A central **Scheduler** resolves the DAG (cycle detection, topological sort) and dispatches "ready" tasks to a pool of workers — routing them the way a rail switchyard routes cars to the right track
- **Redis Streams** (consumer groups) is used directly as the dispatch mechanism — not hidden behind Celery — so heartbeats, lease expiry, retries with backoff, and idempotent task claiming are custom-built
- All state (workflow status, task status, retry counts, timestamps) is persisted in **PostgreSQL**, so the system survives scheduler restarts without losing progress
- Failure injection (killing workers mid-task, simulating partitions) validates that the system actually recovers, not just the happy path
- A live **Angular** dashboard visualizes DAG execution in real time
- Switchyard is domain-agnostic — a "task" is just a unit of code with declared dependencies, parameters, and outputs. It has no built-in knowledge of what any task actually does.
- Supports **compensating transactions** (the saga pattern) — a task can declare what to run if it ultimately fails after exhausting retries, so partially-completed workflows can be cleanly undone rather than left in a broken state.

## 3. Architecture

```
Angular UI (DAG visualization, live status)
        │  REST + WebSocket/polling
        ▼
FastAPI (submit workflows, query status)
        │
        ▼
Scheduler (Python)
  - Parses & validates DAG (cycle detection, topological sort)
  - Determines "ready" tasks (dependencies satisfied)
  - Publishes ready tasks to Redis Stream
  - On task failure after retries exhausted: schedules on_failure compensating task, if declared
        │
        ▼
Redis Stream (consumer group)
        │            │            │
   Worker 1      Worker 2      Worker N   (Python processes, horizontally scalable)
   - claims message, executes task, acknowledges on success
   - heartbeats for liveness detection
   - unacked/expired messages reclaimed by another worker
        │            │            │
        └──────────► PostgreSQL ◄──────────┘
              (workflow state, task state,
               retry history, execution logs)
```

**Production-grade concerns addressed:**
- Durability — state survives restarts
- At-least-once delivery with idempotent task execution
- Retry with exponential backoff + dead-letter handling after max retries
- Worker crash detection via heartbeats/lease expiry, with task reclaiming
- Compensating transactions (saga pattern) for partial-failure cleanup
- Horizontal scalability — add worker processes independently
- Observability — live dashboard, execution logs, per-task timing

## 4. Tech Stack

| Layer | Choice |
|---|---|
| Backend / API | Python, FastAPI |
| Scheduler & Workers | Plain Python processes (custom-built dispatch logic, not Celery) |
| Queue / Dispatch | Redis Streams (consumer groups) |
| Database | PostgreSQL (via SQLAlchemy) |
| Frontend | Angular |
| DAG Visualization | Cytoscape.js (wrapped in an Angular component) |
| Infra | Docker + Docker Compose (Postgres and Redis run as containers — no local install) |
| Testing | Chaos/failure injection scripts, functional test suite |

Entirely free/open-source. No paid infrastructure required for local development or demoing.

## 5. Why Build From Scratch Instead of Using Celery

Celery would solve worker pools, retries, and broker abstraction "for free" — but that means Celery solves the hard distributed-systems problems, not the engineer. Switchyard uses Redis Streams directly as a queue primitive so that heartbeats, lease expiry, idempotent dispatch, and recovery-on-restart are personally designed and defensible in detail — that's the entire point of the project.

## 6. Workflow Definition Format

Workflows are submitted as YAML. Switchyard has no knowledge of what any task's command actually does — a task is just "run this, with these params, after these dependencies succeed." Resolving inputs (e.g. dataset names, file paths) is entirely the responsibility of the task's own code, not the orchestrator.

```yaml
name: example-pipeline
tasks:
  - id: fetch_dataset
    command: fetch_dataset.py
    params:
      dataset_name: movielens-25m
    outputs: [dataset_path]

  - id: preprocess
    command: preprocess.py
    depends_on: [fetch_dataset]
    params:
      input_path: "{{ fetch_dataset.outputs.dataset_path }}"
    retry:
      max_attempts: 3
      backoff: exponential

  - id: cleanup_on_failure
    command: cleanup.py
```

- `depends_on` — defines the DAG edges. Tasks with no shared dependency run in parallel.
- `params` — arbitrary key/value inputs to a task; opaque to Switchyard.
- `outputs` — named values a task produces, referenceable by downstream tasks via `{{ task_id.outputs.field }}` templating.
- `retry` — max attempts and backoff strategy before a task is considered failed.
- `on_failure` — (see below) a compensating task to run if this task fails after retries are exhausted.

### Compensating transactions (saga pattern)

Some workflows need to proceed even if a related step hasn't succeeded yet, with cleanup logic if it ultimately fails — e.g. an order is placed and inventory reserved immediately, while payment is retried asynchronously in parallel; if payment ultimately fails, the reservation must be undone.

```yaml
tasks:
  - id: place_order
  - id: reserve_inventory
    depends_on: [place_order]
    on_failure: release_inventory
  - id: attempt_payment
    depends_on: [place_order]
    retry: {max_attempts: 5, backoff: exponential}
    on_failure: cancel_order
  - id: release_inventory
  - id: cancel_order
    depends_on: [release_inventory]
```

When a task fails after exhausting retries, the Scheduler checks for an `on_failure` field and schedules that compensating task instead of leaving the workflow in a broken state.

## 7. Demo Workflows

Five workflows are included to demonstrate structural variety — linear chains, parallel branches with compensation, gated pipelines with rollback, fan-out/fan-in, and pure fan-in. Each uses small, genuinely-executing scripts (real subprocess calls, real files/DB rows, real failures) rather than simulated steps.

**1. ETL / data pipeline** — linear chain, output → input templating
```
fetch_dataset → clean_transform → validate_schema → load_to_db → notify_completion
```

**2. E-commerce order processing (Amazon-style)** — parallel branches, retries, saga/compensation
```
place_order ─┬─ reserve_inventory (on_failure: release_inventory)
             └─ attempt_payment (retry: 5x, on_failure: cancel_order)
                     ↓ (on success)
              confirm_order → notify_shipping
```

**3. CI/CD deployment pipeline** — gated pipeline with rollback
```
run_unit_tests → build_artifact → deploy_staging → run_smoke_tests → deploy_prod (on_failure: rollback)
```

**4. Media processing pipeline** — fan-out then fan-in
```
upload_video ─┬─ transcode_1080p ─┐
              ├─ transcode_720p   ├─ generate_thumbnail → run_content_moderation → publish
              └─ transcode_480p ──┘
```

**5. Scheduled report generation** — pure fan-in from independent sources
```
fetch_sales_data ─┐
fetch_user_data ──┼─ merge_datasets → generate_pdf_report → upload_to_storage → notify_stakeholders
fetch_marketing_data ┘
```

## 8. Repository Structure

```
switchyard/
├── api/                # FastAPI app — submit workflows, query status
├── scheduler/          # DAG resolution, dispatch loop, on_failure handling
├── worker/             # Task execution, heartbeat, ack/nack logic
├── shared/             # Shared models, DB schema, DAG validation logic
├── workflows/           # Demo workflow YAML definitions + task scripts
│   ├── etl/
│   ├── ecommerce/
│   ├── cicd/
│   ├── media/
│   └── reporting/
├── ui/                  # Angular dashboard
├── chaos/                # Failure injection scripts + results
├── docker-compose.yml
└── README.md
```

## 9. Local Setup

Prerequisites: Docker, Docker Compose, Node.js (for Angular), Python 3.11+.

```bash
git clone <repo-url>
cd switchyard
docker-compose up -d postgres redis   # infra only, no local install needed
docker-compose up --build             # full stack
docker-compose up --scale worker=5    # demonstrate horizontal scaling
```

Angular dashboard: `cd ui && npm install && ng serve`

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: switchyard
      POSTGRES_USER: switchyard
      POSTGRES_PASSWORD: devpassword
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data

  redis:
    image: redis:7
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  scheduler:
    build: ./scheduler
    depends_on: [postgres, redis]
    environment:
      DATABASE_URL: postgresql://switchyard:devpassword@postgres:5432/switchyard
      REDIS_URL: redis://redis:6379

  worker:
    build: ./worker
    depends_on: [postgres, redis]
    deploy:
      replicas: 3
    environment:
      DATABASE_URL: postgresql://switchyard:devpassword@postgres:5432/switchyard
      REDIS_URL: redis://redis:6379

  api:
    build: ./api
    depends_on: [postgres, redis]
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql://switchyard:devpassword@postgres:5432/switchyard
      REDIS_URL: redis://redis:6379

volumes:
  pg_data:
  redis_data:
```

## 10. Build Roadmap (3–4 months)

| Phase | Focus |
|---|---|
| **Month 1** | DAG model + validation (cycle detection, topological sort, parallel branches), single-worker happy-path execution end-to-end, Postgres schema for workflow/task state |
| **Month 2** | Multi-worker pool via Redis Streams consumer groups, retry logic with backoff, idempotent task claiming, state persistence/recovery on scheduler restart, output → input templating |
| **Month 3** | `on_failure` compensating tasks (saga pattern), chaos/failure injection testing (kill workers, simulate partitions, verify recovery), Angular dashboard with live DAG visualization, all 5 demo workflows built out |
| **Month 4** | Polish, real benchmark numbers (throughput, recovery time under failure), technical write-up of design decisions, stretch features (cron-triggered workflows, priority queues, resource-aware scheduling, visual drag-and-drop DAG builder) |

## 11. Engineering Principles for This Codebase

- **Commit incrementally, with real debugging history.** No large "phase-complete" dumps. Commits should reflect actual iteration — write a failing test, fix it, commit. This matters for the project's credibility as genuinely engineered, not agent-generated in bulk.
- **Minimize meta-documentation.** One README, kept current. No `COMPLETION_SUMMARY.md`, `TEST_INVENTORY.md`, or similar — these read as AI-agent scaffolding rather than engineering artifact.
- **Every non-trivial design decision gets a one-paragraph rationale** in code comments or `/docs/decisions/` — e.g. why Postgres over Mongo, why Streams over Pub/Sub, why randomized retry backoff, why on_failure instead of a full saga orchestration framework. These are exactly the questions an interviewer will ask.
- **Benchmark claims must be backed by committed data.** `/chaos/results/` should contain real output from real runs, not aspirational numbers.
- **Demo task scripts are deliberately simple but genuinely functional.** They really execute, really read/write files or DB rows, and really fail on bad input — complexity belongs in the orchestrator, not the demo business logic.

## 12. Working With Claude Code on This Repo

When starting a session in VS Code with Claude Code, point it at this README first — it contains the full architecture, stack, workflow spec, and phase plan. Useful framing for early sessions:

> "Read README.md. We're starting Month 1: DAG model, validation, and single-worker happy-path execution. Let's start with the DAG data model and cycle detection in `shared/`."

Keep sessions scoped to one phase/component at a time rather than asking for the whole system at once — this keeps commits small and reviewable, in line with the engineering principles above.