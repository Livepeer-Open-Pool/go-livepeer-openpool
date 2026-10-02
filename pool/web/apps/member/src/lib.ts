import type { Gpu, MachineGpu, Me, Membership, RegionConfig, RegionResult } from "@openpool/core"
import { regionSections } from "@openpool/ui"
import { Array, Option, pipe } from "effect"
import { html } from "lit"

/** "2 × NVIDIA RTX 4090, 1 × NVIDIA RTX 3090", with models the region no longer lists named by id. */
export const describeGpus = (declared: ReadonlyArray<MachineGpu>, known: ReadonlyArray<Gpu>): string =>
  declared.length === 0
    ? "no GPUs declared"
    : pipe(
        declared,
        Array.map((gpu) => {
          const name = pipe(
            Array.findFirst(known, (k) => k.id === gpu.gpu_id),
            Option.match({ onNone: () => gpu.gpu_id as string, onSome: (k) => k.name }),
          )
          return `${gpu.count} × ${name}`
        }),
        Array.join(", "),
      )

/** Why the address has nothing to show in a region where it is not a member. */
const notAMember = (region: RegionConfig, me: Me) => {
  const latest = Array.head(me.applications)
  return Option.match(latest, {
    onNone: () => html`<p class="muted">You have not applied in ${region.name}. <a href="/apply?region=${region.id}">Apply here</a>.</p>`,
    onSome: (application) =>
      application.status === "pending"
        ? html`<p class="notice warn">Your application in ${region.name} is waiting for the operator.</p>`
        : html`<p class="muted">You are not a member in ${region.name}. <a href="/">See your overview</a>.</p>`,
  })
}

/** One section per region: the member's things where they are a member, and why not where they are not. */
export const memberSections = <A>(
  results: ReadonlyArray<RegionResult<Membership<A>>>,
  section: (region: RegionConfig, detail: A, me: Me) => unknown,
): unknown =>
  regionSections(results, (region, membership) =>
    Option.match(membership.detail, {
      onNone: () => notAMember(region, membership.me),
      onSome: (detail) => section(region, detail, membership.me),
    }),
  )
