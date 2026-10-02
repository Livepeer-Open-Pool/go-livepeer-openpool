import { TaskStatus } from "@lit/task"
import { Api, type Gpu, GpuInput, parseInput, regions as allRegions, type RegionConfig, type RegionResult } from "@openpool/core"
import { actionStatus, badge, formatMemory, regionNotices, regionSections, renderQuery, SitePage } from "@openpool/ui"
import { Effect, Option } from "effect"
import { html, nothing } from "lit"
import { customElement, state } from "lit/decorators.js"
import { count, everywhere, field, inRegion, inRegions, outcomes } from "../lib.js"

const page = Effect.all({ regions: allRegions, gpus: everywhere(Api.gpus) })

const add = (regions: ReadonlyArray<string>, input: unknown) =>
  Effect.flatMap(parseInput(GpuInput, "The GPU model")(input), (gpu) => inRegions(regions, Api.createGpu(gpu)))

/** The GPU models applicants may list, per region. */
@customElement("gpus-page")
export class GpusPage extends SitePage {
  @state() private added: Option.Option<ReadonlyArray<RegionResult<Gpu>>> = Option.none()

  readonly #page = this.query(() => page)
  readonly #refresh = (): void => void this.#page.run()
  readonly #add = this.action(add, (results) => {
    this.added = Option.some(results)
    this.#refresh()
  })
  readonly #accept = this.action(
    (region: string, gpu: Gpu, accepted: boolean) => inRegion(region, Api.updateGpu(gpu.id, { name: gpu.name, vram_mb: gpu.vram_mb, accepted })),
    this.#refresh,
  )

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>GPU models</h1>
          <p class="muted">
            Applicants pick their machines' GPUs from this list, and each app names the models it may run on. Retiring a model
            stops new applications from choosing it and leaves existing machines alone.
          </p>
        </header>
        ${renderQuery(this.#page, ({ regions, gpus }) => html`
          ${this.#addForm(regions)} ${Option.match(this.added, { onNone: () => nothing, onSome: (results) => outcomes(results, "Added") })}
          ${actionStatus(this.#add)} ${actionStatus(this.#accept)} ${regionNotices(gpus)}
          ${regionSections(gpus, (region, detail) =>
            detail.gpus.length === 0
              ? html`<p class="muted">${region.name} lists no GPU models, so nobody can apply there yet.</p>`
              : html`
                  <div class="table-scroll">
                    <table>
                      <caption>Models in ${region.name}</caption>
                      <thead>
                        <tr><th scope="col">Model</th><th scope="col" class="num">Memory</th><th scope="col">New applications</th><th scope="col"><span class="visually-hidden">Change</span></th></tr>
                      </thead>
                      <tbody>
                        ${detail.gpus.map((gpu) => html`
                          <tr>
                            <th scope="row">${gpu.name}</th>
                            <td class="num">${formatMemory(gpu.vram_mb)}</td>
                            <td>${gpu.accepted ? badge("accepted", "good") : badge("retired", "warn")}</td>
                            <td class="actions">
                              <button type="button" class="small" @click=${() => void this.#accept.run([region.id, gpu, !gpu.accepted])}>
                                ${gpu.accepted ? "Retire" : "Accept again"}<span class="visually-hidden"> ${gpu.name} in ${region.name}</span>
                              </button>
                            </td>
                          </tr>
                        `)}
                      </tbody>
                    </table>
                  </div>
                `,
          )}
        `)}
      </div>
    `
  }

  #addForm(regions: ReadonlyArray<RegionConfig>) {
    return html`
      <form class="card" @submit=${(event: SubmitEvent) => this.#submit(event, regions)}>
        <h2>Add a model</h2>
        <div class="fields">
          <label>
            Model name
            <input name="name" required maxlength="100" placeholder="NVIDIA GeForce RTX 5090" />
            <span class="hint">As <code>nvidia-smi</code> names it, so it matches what agents report.</span>
          </label>
          <label>
            Memory in MB
            <input type="number" name="vram_mb" required min="1024" step="1" inputmode="numeric" placeholder="32607" />
          </label>
          <label>
            Add to
            <select name="where">
              <option value="">Every region</option>
              ${regions.map((region) => html`<option value=${region.id}>${region.name} only</option>`)}
            </select>
          </label>
        </div>
        <p><button type="submit" class="primary" ?disabled=${this.#add.status === TaskStatus.PENDING}>Add model</button></p>
      </form>
    `
  }

  #submit(event: SubmitEvent, regions: ReadonlyArray<RegionConfig>): void {
    event.preventDefault()
    const form = event.target as HTMLFormElement
    const data = new FormData(form)
    const where = field(data, "where")
    this.added = Option.none()
    void this.#add
      .run([where === "" ? regions.map((r) => r.id) : [where], { name: field(data, "name"), vram_mb: count(data, "vram_mb") }])
      .then(() => {
        if (this.#add.status === TaskStatus.COMPLETE) form.reset()
      })
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gpus-page": GpusPage
  }
}
