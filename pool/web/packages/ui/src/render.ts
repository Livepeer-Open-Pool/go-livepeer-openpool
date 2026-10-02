import type { Task } from "@lit/task"
import { TaskStatus } from "@lit/task"
import { type RegionConfig, type RegionResult, unanswered } from "@openpool/core"
import { Either } from "effect"
import { html, nothing, type TemplateResult } from "lit"
import { asFailure, describeFailure, type Failure } from "./runtime.js"

// How every page shows the same situations: loading, failed, a region that
// did not answer, a status.

export const skeleton = (): TemplateResult => html`<div class="skeleton" role="status" aria-label="Loading"></div>`

/** What went wrong, with a way to try again when there is one. */
export const failureNotice = (failure: Failure, retry?: () => void): TemplateResult => html`
  <div class="notice bad" role="alert">
    <p>${describeFailure(failure)}</p>
    ${retry ? html`<p><button type="button" class="small" @click=${retry}>Try again</button></p>` : nothing}
  </div>
`

/**
 * A query's value, or its loading or failed state. While a query that has
 * already answered runs again, its last answer stays on the page instead of
 * flashing back to a placeholder.
 */
export const renderQuery = <A>(task: Task<ReadonlyArray<unknown>, A>, complete: (value: A) => unknown): unknown => {
  if (task.status === TaskStatus.PENDING && task.value !== undefined) {
    return complete(task.value)
  }
  return task.render({
    initial: skeleton,
    pending: skeleton,
    complete,
    error: (thrown) => failureNotice(asFailure(thrown), () => void task.run()),
  })
}

/** What an action is doing, or why it failed. Nothing once it is done or not started. */
export const actionStatus = (task: Task<ReadonlyArray<unknown>, unknown>, working = "Working…"): unknown =>
  task.render({
    initial: () => nothing,
    pending: () => html`<p class="muted" role="status">${working}</p>`,
    complete: () => nothing,
    error: (thrown) => failureNotice(asFailure(thrown)),
  })

/** The regions that did not answer, named, so a partial page says it is partial. */
export const regionNotices = <A>(results: ReadonlyArray<RegionResult<A>>): unknown => {
  const missing = unanswered(results)
  if (missing.length === 0) return nothing
  return html`
    <div class="notice warn" role="status">
      <p>
        ${missing.length === results.length ? "No region answered." : "Not every region answered; what is shown comes from the ones that did."}
      </p>
      <ul>
        ${missing.map(({ region, error }) => html`<li><strong>${region.name}</strong>: ${describeFailure(error)}</li>`)}
      </ul>
    </div>
  `
}

/** One section per region that answered, under the region's name. */
export const regionSections = <A>(
  results: ReadonlyArray<RegionResult<A>>,
  section: (region: RegionConfig, value: A) => unknown,
): unknown =>
  results.map(({ region, result }) =>
    Either.match(result, {
      onLeft: () => nothing,
      onRight: (value) => html`
        <section class="stack" aria-labelledby="region-${region.id}">
          <h2 id="region-${region.id}">${region.name}</h2>
          ${section(region, value)}
        </section>
      `,
    }),
  )

type Tone = "good" | "warn" | "bad" | "plain"

const tones: Readonly<Record<string, Tone>> = {
  active: "good", approved: "good", sent: "good", confirmed: "good", running: "good", healthy: "good", ready: "good", passed: "good", offered: "good",
  pending: "warn", probation: "warn", draining: "warn", open: "warn", sending: "warn", signed: "warn", broadcast: "warn",
  upgrading: "warn", paused: "warn", "on hold": "warn", restarting: "warn", starting: "warn",
  suspended: "bad", banned: "bad", rejected: "bad", failed: "bad", blocked: "bad", removed: "bad", exited: "bad", unhealthy: "bad", dead: "bad",
}

/** A status as a badge whose color says whether it needs attention. */
export const badge = (status: string, tone?: Tone): TemplateResult =>
  html`<span class="badge ${tone ?? tones[status] ?? "plain"}">${status}</span>`
