import { TaskStatus } from "@lit/task"
import { Api, type App, AppInput, AppVersionInput, type Container, type Gpu, parseInput } from "@openpool/core"
import { actionStatus, formatMemory, navigate, renderQuery, SitePage } from "@openpool/ui"
import { Effect, Option } from "effect"
import { css, html, nothing } from "lit"
import { customElement, property, state } from "lit/decorators.js"
import { count, field, inRegion, lines, pairs } from "../lib.js"

interface Loaded {
  readonly gpus: ReadonlyArray<Gpu>
  /** The app being edited; none when a new one is being made. */
  readonly existing: Option.Option<App>
  /** What a new app's form starts from: an app of another region, with that region's GPU models by name. */
  readonly template: Option.Option<{ readonly app: App; readonly gpuNames: ReadonlyArray<string> }>
}

const find = (region: string, id: string) =>
  Effect.map(inRegion(region, Api.apps), ({ apps }) => Option.fromNullable(apps.find((app) => app.id === id)))

const load = (region: string, id: string, from: string) =>
  Effect.gen(function* () {
    const { gpus } = yield* inRegion(region, Api.gpus)
    if (id !== "new") {
      return { gpus, existing: yield* find(region, id), template: Option.none() } satisfies Loaded
    }
    const [fromRegion, fromId] = from.split(":")
    if (fromRegion === undefined || fromId === undefined) {
      return { gpus, existing: Option.none(), template: Option.none() } satisfies Loaded
    }
    const source = yield* find(fromRegion, fromId)
    const sourceGpus = (yield* inRegion(fromRegion, Api.gpus)).gpus
    return {
      gpus,
      existing: Option.none(),
      template: Option.map(source, (app) => ({
        app,
        gpuNames: sourceGpus.filter((gpu) => app.eligible_gpus.includes(gpu.id)).map((gpu) => gpu.name),
      })),
    } satisfies Loaded
  })

const save = (region: string, existing: Option.Option<App>, input: unknown) =>
  Effect.flatMap(parseInput(AppInput, "The app")(input), (app) =>
    inRegion(
      region,
      Option.match(existing, { onNone: () => Api.createApp(app), onSome: (current) => Api.updateApp(current.id, app) }),
    ),
  )

const release = (region: string, app: App, input: unknown) =>
  Effect.flatMap(parseInput(AppVersionInput, "The version")(input), (version) => inRegion(region, Api.setAppVersion(app.id, version)))

const containerFrom = (data: FormData) => ({
  image: field(data, "image"),
  command: lines(data.get("command")),
  env: pairs(data.get("env"), "="),
  port: count(data, "port"),
  health_path: field(data, "health_path"),
  gpus: count(data, "container_gpus"),
  no_gpu: data.has("no_gpu"),
  shm_size: field(data, "shm_size"),
  cache_path: field(data, "cache_path"),
})

/** Making an app, changing one, and releasing a new version of one. */
@customElement("app-page")
export class AppPage extends SitePage {
  static override styles = [
    ...SitePage.styles,
    css`
      textarea.code, input.code { font-family: var(--font-mono); font-size: var(--step--1); }
    `,
  ]

  @property() region = ""
  @property() app = ""
  @state() private saved = false

  readonly #loaded = this.query(load, () => [this.region, this.app, new URLSearchParams(location.search).get("from") ?? ""] as const)
  readonly #save = this.action(save, (app, _region, existing) => {
    if (Option.isNone(existing)) {
      navigate(`/apps/${this.region}/${app.id}`)
    } else {
      this.saved = true
      void this.#loaded.run()
    }
  })
  readonly #release = this.action(release, () => {
    this.saved = true
    void this.#loaded.run()
  })

  override render() {
    return html`
      <div class="stack">
        <p><a href="/apps">All apps</a></p>
        ${renderQuery(this.#loaded, (loaded) =>
          this.app !== "new" && Option.isNone(loaded.existing)
            ? html`<p class="notice bad">There is no app with this address in ${this.region}.</p>`
            : this.#form(loaded),
        )}
      </div>
    `
  }

  #form({ gpus, existing, template }: Loaded) {
    const start = Option.orElse(existing, () => Option.map(template, (t) => t.app))
    const value = <K extends keyof App>(key: K, fallback: App[K]): App[K] => Option.match(start, { onNone: () => fallback, onSome: (app) => app[key] })
    const eligible = (gpu: Gpu): boolean =>
      Option.match(existing, {
        onSome: (app) => app.eligible_gpus.includes(gpu.id),
        onNone: () => Option.match(template, { onNone: () => false, onSome: (t) => t.gpuNames.includes(gpu.name) }),
      })
    const test = value("test", { method: "GET", path: "", headers: {}, body: "", expect_status: 200, expect_contains: [], timeout_ms: 10000 })
    const price = value("price", { price: "", unit: "hour" })
    const isNew = Option.isNone(existing)
    return html`
      <header class="stack-s">
        <h1>${Option.match(existing, { onNone: () => `New app in ${this.region}`, onSome: (app) => app.name })}</h1>
        ${Option.match(template, { onNone: () => nothing, onSome: (t) => html`<p class="notice">Started from <code translate="no">${t.app.name}</code> in another region. Nothing is saved until you save.</p>` })}
        ${this.saved ? html`<p class="notice good" role="status">Saved.</p>` : nothing}
      </header>

      <form @submit=${(event: SubmitEvent) => this.#submit(event, existing, gpus)}>
        <fieldset>
          <legend>What customers ask for</legend>
          <div class="fields">
            <label>
              Name
              <input class="code" name="name" required maxlength="200" .value=${value("name", "")} ?readonly=${!isNew} placeholder="vllm/qwen2.5-7b-instruct" />
              <span class="hint">The Live Runner app name. ${isNew ? "It cannot be changed later." : "It cannot be changed."}</span>
            </label>
            <label>
              Mode
              <select name="mode" ?disabled=${!isNew}>
                <option value="single-shot" ?selected=${value("mode", "single-shot") === "single-shot"}>single-shot: one request, one answer</option>
                <option value="persistent" ?selected=${value("mode", "single-shot") === "persistent"}>persistent: a session that stays open</option>
              </select>
              ${isNew ? nothing : html`<input type="hidden" name="mode" value=${value("mode", "single-shot")} />`}
            </label>
            <label>
              Price in US dollars
              <input name="price" required inputmode="decimal" pattern="[0-9]*[.]?[0-9]+" .value=${price.price} placeholder="0.50" />
            </label>
            <label>
              Charged
              <select name="unit">
                <option value="hour" ?selected=${price.unit === "hour"}>per hour of session</option>
                <option value="720p" ?selected=${price.unit === "720p"}>per 720p-equivalent hour</option>
                <option value="fixed" ?selected=${price.unit === "fixed"}>per request</option>
              </select>
            </label>
          </div>
          <label>
            Description
            <textarea name="description" maxlength="2000">${value("description", "")}</textarea>
            <span class="hint">Shown to members and on the public site.</span>
          </label>
          <label>
            Discovery text
            <textarea name="discovery" maxlength="2000">${value("discovery", "")}</textarea>
            <span class="hint">What customers see when they discover the app on the orchestrator.</span>
          </label>
        </fieldset>

        <fieldset>
          <legend>Which machines may run it</legend>
          <div class="stack-s" role="group" aria-label="Eligible GPU models">
            ${gpus.length === 0 ? html`<p class="muted">This region lists no GPU models. <a href="/gpus">Add some</a>; an app with none chosen may run on any machine.</p>` : nothing}
            ${gpus.map((gpu) => html`
              <label class="inline">
                <input type="checkbox" name="gpu:${gpu.id}" ?checked=${eligible(gpu)} />
                <span>${gpu.name} <span class="hint">${formatMemory(gpu.vram_mb)}</span></span>
              </label>
            `)}
          </div>
          <div class="fields">
            <label>
              Sessions at once, by default
              <input type="number" name="default_capacity" required min="1" .value=${String(value("default_capacity", 1))} />
            </label>
            <label>
              Sessions at once, at most
              <input type="number" name="max_capacity" required min="1" .value=${String(value("max_capacity", 1))} />
            </label>
          </div>
        </fieldset>

        ${isNew
          ? html`
              <fieldset>
                <legend>Version and container</legend>
                <label>
                  Version
                  <input name="required_version" required maxlength="100" .value=${value("required_version", "")} placeholder="1.0.0" />
                  <span class="hint">Runners report it; the pool admits only the version it told the machine to run.</span>
                </label>
                ${this.#containerFields(Option.map(start, (app) => app.container))}
              </fieldset>
            `
          : nothing}

        <fieldset>
          <legend>Test</legend>
          <p class="hint">
            The pool sends this request to each runner through the orchestrator, as a customer would. A runner serves customers
            only after it passes. Leave the path empty for no test: such an app's runners wait for you to activate them by hand.
          </p>
          <div class="fields">
            <label>
              Method
              <select name="test_method">
                ${["GET", "POST"].map((method) => html`<option ?selected=${test.method === method}>${method}</option>`)}
              </select>
            </label>
            <label>
              Path
              <input class="code" name="test_path" .value=${test.path} placeholder="/v1/models" />
            </label>
            <label>
              Expected status
              <input type="number" name="test_status" min="100" max="599" .value=${String(test.expect_status)} />
            </label>
            <label>
              Time limit in milliseconds
              <input type="number" name="test_timeout" min="100" .value=${String(test.timeout_ms)} />
            </label>
          </div>
          <label>
            Headers <span class="hint">one per line, as Name: value</span>
            <textarea class="code" name="test_headers">${Object.entries(test.headers).map(([k, v]) => `${k}: ${v}`).join("\n")}</textarea>
          </label>
          <label>
            Body
            <textarea class="code" name="test_body">${test.body}</textarea>
          </label>
          <label>
            The answer must contain <span class="hint">one text per line</span>
            <textarea class="code" name="test_contains">${test.expect_contains.join("\n")}</textarea>
          </label>
        </fieldset>

        <p><button type="submit" class="primary" ?disabled=${this.#save.status === TaskStatus.PENDING}>${isNew ? "Create app" : "Save changes"}</button></p>
        ${actionStatus(this.#save)}
      </form>

      ${Option.match(existing, { onNone: () => nothing, onSome: (app) => this.#versionForm(app) })}
    `
  }

  #containerFields(container: Option.Option<Container>) {
    const c = Option.getOrElse(container, (): Container => ({ image: "", command: [], env: {}, port: 8000, health_path: "/health", gpus: 0, no_gpu: false, shm_size: "", cache_path: "" }))
    return html`
      <label>
        Image
        <input class="code" name="image" required .value=${c.image} placeholder="vllm/vllm-openai@sha256:…" />
        <span class="hint">Pinned by digest, as name@sha256:… A tag can change under you; a digest cannot.</span>
      </label>
      <div class="fields">
        <label>
          Port the app listens on
          <input type="number" name="port" required min="1" max="65535" .value=${String(c.port)} />
        </label>
        <label>
          Health path
          <input class="code" name="health_path" .value=${c.health_path} placeholder="/health" />
          <span class="hint">Answers 200 while the app can take work. Empty means it is not checked.</span>
        </label>
        <label>
          GPUs
          <input type="number" name="container_gpus" min="0" .value=${String(c.gpus)} />
          <span class="hint">0 gives the container all of the machine's GPUs.</span>
        </label>
        <label>
          Shared memory
          <input name="shm_size" .value=${c.shm_size} placeholder="8g" />
        </label>
        <label>
          Cache path
          <input class="code" name="cache_path" .value=${c.cache_path} placeholder="/cache" />
          <span class="hint">Model files kept here survive restarts and new versions.</span>
        </label>
      </div>
      <label class="inline">
        <input type="checkbox" name="no_gpu" ?checked=${c.no_gpu} />
        <span>This app needs no GPU</span>
      </label>
      <label>
        Arguments <span class="hint">one per line</span>
        <textarea class="code" name="command">${c.command.join("\n")}</textarea>
      </label>
      <label>
        Environment <span class="hint">one per line, as NAME=value. Do not put secrets here: members can read them.</span>
        <textarea class="code" name="env">${Object.entries(c.env).map(([k, v]) => `${k}=${v}`).join("\n")}</textarea>
      </label>
    `
  }

  #versionForm(app: App) {
    return html`
      <form class="card" @submit=${(event: SubmitEvent) => this.#submitVersion(event, app)}>
        <h2>Release a new version</h2>
        <p class="muted">
          A version is its name and its container, changed together. Now at <strong>${app.required_version}</strong>. A new
          version reaches machines a few at a time, idle ones first, each after its sessions have finished, so the app is never
          without runners.
        </p>
        <label>
          New version
          <input name="version" required maxlength="100" placeholder="next after ${app.required_version}" />
        </label>
        ${this.#containerFields(Option.some(app.container))}
        <p><button type="submit" ?disabled=${this.#release.status === TaskStatus.PENDING}>Release version</button></p>
        ${actionStatus(this.#release)}
      </form>
    `
  }

  #submit(event: SubmitEvent, existing: Option.Option<App>, gpus: ReadonlyArray<Gpu>): void {
    event.preventDefault()
    const data = new FormData(event.target as HTMLFormElement)
    this.saved = false
    const input = {
      name: field(data, "name"),
      mode: field(data, "mode"),
      price: { price: field(data, "price"), unit: field(data, "unit") },
      required_version: Option.match(existing, { onNone: () => field(data, "required_version"), onSome: (app) => app.required_version }),
      container: Option.match(existing, { onNone: () => containerFrom(data), onSome: (app) => app.container }),
      eligible_gpus: gpus.filter((gpu) => data.has(`gpu:${gpu.id}`)).map((gpu) => gpu.id),
      default_capacity: count(data, "default_capacity"),
      max_capacity: count(data, "max_capacity"),
      test: {
        method: field(data, "test_method"),
        path: field(data, "test_path"),
        headers: pairs(data.get("test_headers"), ":"),
        body: String(data.get("test_body") ?? ""),
        expect_status: count(data, "test_status"),
        expect_contains: lines(data.get("test_contains")),
        timeout_ms: count(data, "test_timeout"),
      },
      discovery: field(data, "discovery"),
      description: field(data, "description"),
    }
    void this.#save.run([this.region, existing, input])
  }

  #submitVersion(event: SubmitEvent, app: App): void {
    event.preventDefault()
    const form = event.target as HTMLFormElement
    const data = new FormData(form)
    this.saved = false
    void this.#release.run([this.region, app, { version: field(data, "version"), container: containerFrom(data) }])
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "app-page": AppPage
  }
}
