# Code Structure and Style Guidelines

### File Length and Structure

- Never allow a file to exceed **500 lines**.
- If a file approaches **400 lines**, break it up immediately into smaller, logically grouped files.
- Treat **1000 lines as unacceptable**, even temporarily.
- Use clear **naming conventions** to keep small files logically grouped.
- Each file should have a **clear, single responsibility**.
- Use **MARK** comments to separate sections within files for better navigation.
- Use **extensions** to group related methods and improve readability within a file.
- Maintain consistent code formatting (indentation, line spacing, and brace styles). VSCode should be configured with a formatter/prettier/linter so style is enforced automatically.

### Each feature or bug should be implemented in its own branch.
- Branch names should be descriptive and follow a consistent naming convention (e.g., `feature/clear-naming-conventions`, `bugfix/fix-login-error`).
- Ensure branches are regularly updated with the main branch to avoid merge conflicts.
- Use pull requests for code reviews and ensure that all changes are reviewed before merging into the main branch.
- Write clear and concise commit messages that describe the changes made.
- Avoid large commits; aim for small, logical commits that are easy to review.
- Regularly delete branches that have been merged to keep the repository clean.
- Always test code in branches before merging to ensure stability.

### API Implementation

- Follow RESTful principles for API design.
- Use consistent naming conventions for endpoints, parameters, and responses.
- Ensure proper error handling and return appropriate HTTP status codes.
- Document API endpoints using tools like Swagger or Postman.
- Use versioning for APIs to manage changes and maintain backward compatibility.
- Secure APIs with authentication and authorization mechanisms.
- Write unit tests for API endpoints to ensure functionality and reliability.
- Use pagination for endpoints that return large datasets.
- Optimize API performance by minimizing payload size and using efficient data structures.
- Use caching strategies to improve response times for frequently accessed data.
- Monitor API usage and performance using tools like API Gateway or custom logging solutions.
- Regularly review and update API documentation to reflect changes and new features.
- Official docs/portal (reference, guides, changelog/release notes).
- Official spec (OpenAPI/Swagger, GraphQL SDL), SDK READMEs.
- Auth pages (OAuth scopes, API keys, service accounts), rate limits.
- Error catalogs, pagination rules, webhooks/callbacks, idempotency policy.
- Base URL(s), versions, deprecation notices, sandbox vs prod.
- Auth method(s): API key, OAuth2 grant types, token lifetimes, scopes.
- Rate limits: global vs per-endpoint, concurrency, burst rules, headers to read.
- Pagination: cursor vs page/limit, known edge cases (empty pages, duplicates).
- Error model: status codes, body shape, retryable vs non-retryable.
- Idempotency: header/key format, which verbs support it.
- Data contracts: request/response schemas, enum values, nullable fields.
- Webhooks: event types, signature verification, replay/ordering guarantees.
- SLAs: latency, uptime, maintenance windows.
- SDK vs. raw HTTP: prefer official SDK if mature; otherwise typed client from OpenAPI.
- Auth flow: where tokens live, rotation, secrets manager, least-privilege scopes.
- Pagination strategy: loop until next is null; guard against duplicates.
- Retries: exponential backoff w/ jitter, cap tries; honor Retry-After.
- Idempotency: generate a UUID per create/update; store keys for replay safety.
- Schema validation: strict request/response validation (zod/pydantic).
- Observability: metrics, tracing, structured logs, redaction rules.
- Testing: unit (mocked), contract (schema), integration (sandbox), load (rate-limit safe).
- Rollout: feature flag → canary → full; backout plan.
- Spec ingestion & typed client generation (if applicable).
- Auth module + secret management.
- Happy-path endpoints (read) → write ops (create/update) with idempotency.
- Pagination & rate-limit controller.
- Error mapping & retry policy.
- Webhook receiver + signature verification.
- Tests & fixtures; Postman/Insomnia collection.
- Observability & dashboards.
- Docs/readme + examples.

### MCP (Model Context Protocol) Architecture

- Restate the user’s goal before acting; stop if the goal is ambiguous and ask one clarifying question.
- Specify whether the Agent may: (a) propose actions only, (b) execute low-risk actions, or (c) fully automate within quotas.
- Always enumerate capabilities before use (e.g., list tools/resources; read each tool’s schema and examples).
- Populate exactly what the tool schema requires; if unclear, request the missing field(s) once.
- Prefer server-side filters/queries over client-side scanning; pull only the fields you need.
- For large content, stream or request ranges; avoid fetching full blobs unless required.
- When using external APIs, handle rate limits and errors gracefully.
- Log actions and decisions for auditing and debugging.
- Regularly review and update the MCP architecture to ensure it meets evolving requirements and best practices.
- When information could be stale (news, prices, schedules), re-query the authoritative tool before answering.
- Prefer quoting structured fields returned by tools; do not invent fields or enums.
- Never echo secrets, tokens, or raw PII back to the user unless explicitly asked and permitted. Mask by default.
- Log tool calls (name, params summary, IDs, timestamps) and decisions for post-hoc review.


### Responsibility and Modularity

- Every functionality should exist in a **dedicated class, struct, or protocol**, even if small.
- Follow the **Single Responsibility Principle**:
  - Each file must focus on a **single responsibility**.
  - Each view, manager, or view model should have one clear role.
  - If something feels like it “does two things,” split it immediately.
- Code should be modular and **reusable like Lego blocks**:
  - Interchangeable, testable, and isolated.
  - Ask: “Can this class be reused in a different screen or project?” If not, refactor.
- Favor **composition over inheritance**, but always use object-oriented thinking.
- Use protocols to define clear interfaces and enable mocking in tests.
- Avoid deep nesting of conditionals and loops; refactor into smaller functions if necessary.

### Manager and Coordinator Patterns

- **ViewModel** → Handles UI-specific business logic.
- **Manager** → Handles general business logic, services, or external dependencies.
- **Coordinator** → Handles navigation and state flow between screens.
- Avoid tightly coupling business logic directly into Views. Views should be kept lightweight.
- Use protocols to define clear interfaces and enable mocking in tests.
- Managers should not directly manipulate UI elements.
- Coordinators should not contain business logic; they should delegate to Managers or ViewModels.
- Use dependency injection to provide Managers and ViewModels to Views and Coordinators.
- Ensure that each Manager, ViewModel, and Coordinator has a clear, singular purpose.
- Avoid “God classes” or “God functions” that know too much or do too many things.

### Function and Class Size

- Keep functions under **30–40 lines** (48 lines maximum).
- If a class gets over **200 lines**, assess whether it needs splitting into helper classes or extracting shared utilities.
- Avoid “God classes” or “God functions” that know too much or do too many things.
- Each class should have a clear, singular purpose.
- Refactor large methods into smaller, focused helper methods.
- Use extensions to organize related methods and improve readability.
- Use protocols to define clear interfaces and enable mocking in tests.

### Naming and Readability

- All class, method, and variable names must be **clear and descriptive**.
- Avoid short or non-descriptive names like `x`, `tmp`, or `data`. Use intent-revealing names.
- Methods should read like verbs or actions (`fetchUser`, `calculateTotal`).
- Classes and structs should read like nouns (`UserManager`, `CheckoutCoordinator`).
- Use **camelCase** for variables and methods, **PascalCase** for types (classes, structs, enums).
- Use **consistent indentation** (4 spaces per indent level).
- Limit line length to **100 characters** for better readability.
- Use whitespace effectively to separate logical blocks of code.
- Group related properties and methods together within classes.
- Use MARK comments to separate sections within files for better navigation.
- Use comments to explain **why** something is done, not **what** is done (the code should be self-explanatory).
- Avoid commented-out code; remove it instead.
- Use documentation comments (`///`) for public APIs and complex logic.
- Avoid deep nesting; refactor into smaller functions if necessary.
- Use guard statements to handle early exits and reduce pyramid of doom.

### Code Architecture and Reuse

- Favor **dependency injection** to reduce tight coupling between components.
- Write code for reuse and scaling, not just to “make it work.”
- Always separate concerns to maximize testability and maintainability.
- Use protocols to define clear interfaces and enable mocking in tests.
- Avoid deep nesting of conditionals and loops; refactor into smaller functions if necessary.
- Use guard statements to handle early exits and reduce pyramid of doom.
- Prefer `switch` statements over multiple `if-else` chains when dealing with multiple conditions.
- Use `enum` types to represent related constants and states instead of raw values.
- Leverage Swift’s powerful type system to enforce invariants and reduce runtime errors.
- Use `struct` for value types and `class` for reference types, based on the use case.
- Use `final` for classes that are not intended to be subclassed to improve performance and clarity.
- Use `lazy` properties for expensive computations that may not be needed immediately.
- Use `private` and `fileprivate` access control to encapsulate implementation details.
- Use `typealias` to create meaningful names for complex types or closures.

### Additional Best Practices

- Keep **protocols small and focused**, prefer fine-grained responsibilities over massive ones.
- Ensure error handling is clear and consistent, never swallowed silently.
- Write **unit tests** for business logic in Managers and ViewModels.
- Use **extensions** to group related methods and improve readability within a file.
- Maintain consistent code formatting (indentation, line spacing, and brace styles). Cursor should be configured with a formatter/prettier/linter so style is enforced automatically.
- Regularly refactor code to improve structure, readability, and performance.
- Document complex logic with comments, but avoid obvious comments that state the obvious.
- Use version control effectively, with clear commit messages and logical commit sizes.
- Regularly review and update code to adhere to evolving best practices and project requirements.
- Strive for simplicity and clarity in all code, avoiding unnecessary complexity.
- Always consider the future maintainability of the code you write today.
- Embrace feedback and code reviews as opportunities for growth and improvement.
- Continuously learn and adapt to new technologies, frameworks, and methodologies in software development.
- Prioritize user experience and performance in all aspects of development.
- Collaborate effectively with team members, sharing knowledge and best practices.
- Maintain a positive and proactive attitude towards problem-solving and innovation.
- Do not create any new files that exceed 500 lines of code.
- Do not create any new files that are not logically grouped with other files.
- Do not create any new files that do not have a clear, single responsibility.
- Do not create any new files that do not follow the naming conventions used in the project.
- Do not create any new files that do not follow the coding style and structure guidelines outlined above.
- Do not create any new files that do not follow the principles of modularity and reusability.
- Do not create any new markdown files to document code style or structure guidelines.
- Do not create any new files that do not follow the principles of dependency injection and separation of concerns.
- Do not create any new files that do not follow the principles of testability and maintainability.
- Do not create any new files that do not follow the principles of simplicity and clarity.
- Do not create any new files that do not follow the principles of collaboration and knowledge sharing.
- Ensure security is a priority in all code, following best practices for data protection and privacy.
- Do not create any new files that do not follow the principles of performance optimization and efficiency.
- Do not create any new files that do not follow the principles of user experience and accessibility.

# 🤖 MCP Multi-Agent Implementation Instructions

## 🎯 Purpose
Define exact behavioral rules for the **Main Agent** and its **Sub-Agents** to collaborate across complex tasks such as API documentation, UI/UX, code review, QA, and security — ensuring modular specialization, traceability, and structured reporting.

---

## 🧩 1. Main Agent Core Responsibilities
- **Task Interpretation**
  - Parse every user request into clear objectives.
  - Identify task domains (UI/UX, Code, QA, Docs, Security, etc.).
  - Decide if delegation is needed (see Delegation Logic below).

- **Delegation Control**
  - If multiple domains detected, **spawn sub-agents** with scoped instructions.
  - Track all sub-agents and their assigned responsibilities.

- **Context Summarization**
  - Before any implementation, the Main Agent must:
    1. Search and read all relevant documentation, specs, and references.
    2. Summarize findings for each domain (auth, rate limits, APIs, etc.).
    3. Provide sub-agents with this summarized context.

- **Synthesis**
  - Aggregate sub-agent outputs.
  - Merge insights into one cohesive final report.
  - Highlight dependencies, risks, and next actions.

---

## ⚙️ 2. Delegation Logic (Decision Tree)

| Detected Task Type | Action | Assigned Sub-Agent | Scope |
|--------------------|---------|--------------------|--------|
| **API Documentation / Review** | Delegate | `docs_subagent` | Collect docs, confirm endpoint completeness, identify missing sections (auth, pagination, rate limits). |
| **UI/UX Design or Review** | Delegate | `ui_ux_subagent` | Generate mockups, suggest design flows, enforce consistency and accessibility. |
| **Code Implementation / Review** | Delegate | `code_subagent` | Implement or review code logic, enforce structure, detect errors. |
| **QA / Testing** | Delegate | `qa_subagent` | Write test plans, simulate requests, verify results, produce coverage metrics. |
| **Security / Compliance** | Delegate | `security_subagent` | Verify scopes, auth tokens, rate-limiting enforcement, log redaction. |
| **Single-Domain Task** | Execute directly | Main Agent | Proceed without sub-agent delegation. |

---

## 🧱 3. Execution Workflow

### **Step 1 — Context Gathering**
Main Agent performs a documentation-first search:
- Locate **official references**, **OpenAPI specs**, **auth details**, **rate-limit policies**, **error codes**, and **webhooks**.
- Summarize all findings before implementation.

**Collected Fields**
- Base URLs, versioning, sandbox vs. prod  
- Auth methods, scopes, token lifetimes  
- Rate limits, retry headers, concurrency  
- Pagination style and edge cases  
- Error handling (retryable vs fatal)  
- Idempotency headers and supported verbs  
- Webhook schema and signing methods  
- SLAs and uptime commitments  

### **Step 2 — Sub-Agent Delegation**
- The Main Agent analyzes task complexity:
  - If multiple domains are found → **spawn sub-agents**.
  - Pass the summarized context and clear goals to each.

Example:
```yaml
subagents:
  - name: docs_subagent
    role: "API documentation analysis"
    task: "Verify endpoint completeness and update missing sections."
  - name: code_subagent
    role: "Implementation review"
    task: "Validate rate-limit logic and add missing pagination handlers."
  - name: qa_subagent
    role: "Testing and validation"
    task: "Generate automated tests for all modified endpoints."

Step 3 — Sub-Agent Execution

Each Sub-Agent:

Operates autonomously within its scope.

Searches or analyzes relevant context.

Produces a structured JSON report:

{
  "subagent": "code_subagent",
  "summary": "Pagination logic validated, retry loop added.",
  "tasks_completed": ["implemented backoff", "tested pagination cursor"],
  "issues_found": ["duplicate record handling"],
  "next_steps": ["update docs_subagent to document retry headers"]
}

Step 4 — Main Agent Aggregation

Main Agent:

Waits for all Sub-Agents to complete.

Collects their structured outputs.

Synthesizes a unified summary.

Final aggregation example:

{
  "project_summary": {
    "code": "All endpoints validated and tested.",
    "docs": "Auth and rate-limit sections updated.",
    "ui_ux": "Mockup complete; awaiting QA feedback."
  },
  "pending_tasks": [
    "QA validation of new pagination handler",
    "Add rate-limit monitoring widget in dashboard"
  ],
  "next_steps": [
    "Merge updates to staging",
    "Trigger automated nightly eval run"
  ]
}
4. Reporting and Feedback Flow

Sub-Agent → Main Agent Reporting

Each Sub-Agent must return:

summary

tasks_completed

issues_found

next_steps

Main Agent → User Output

Compile all sub-agent reports.

Create unified human-readable Markdown output:

# 🧾 Unified Project Summary
## 📊 Overview
✅ Implementation: 95% Complete  
⚙️ Pending Actions: QA verification, UI review

## 💻 Code
- Retry/backoff implemented  
- Pagination edge cases handled

## 📘 Documentation
- Updated API reference and webhook schema  
- Added rate-limit header details  

## 🧪 QA
- Coverage 88%, regression pass 96%  
- Latency tests pending for 429 scenarios

## 🎨 UI/UX
- Final dashboard prototype complete  
- Awaiting feedback integration  

## 🔮 Next Steps
1. Run load tests under staging  
2. Merge changes post-QA approval  
3. Deploy release v1.1  
```

7. Sub-Agent Naming Convention
Sub-Agent	Domain	Typical Output
ui_ux_subagent	Design	Wireframes, accessibility feedback, layout audit
code_subagent	Development	Code diffs, logic validation, performance notes
qa_subagent	Testing	Test reports, regression summaries, coverage metrics
docs_subagent	Documentation	API docs updates, missing spec detection
security_subagent	Compliance	Scope verification, token security, privacy audit

Always read + summarize documentation before any implementation.

Spawn sub-agents automatically when multi-domain tasks are detected.

Each sub-agent reports back in JSON {summary, tasks, next_steps}.

Main Agent merges all outputs and formats unified report in Markdown.

Maintain clear traceability, consistency, and context integrity.

Return final output to user only after all sub-reports are processed.