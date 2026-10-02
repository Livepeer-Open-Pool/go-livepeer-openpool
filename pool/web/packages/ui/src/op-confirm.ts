import { Option } from "effect"
import { html, LitElement, nothing, type PropertyValues } from "lit"
import { customElement, property, query } from "lit/decorators.js"
import { base, controls, dialogs } from "./styles.js"

/** Something a person is asked to confirm before it is done. */
export interface ConfirmRequest {
  readonly heading: string
  /** What will happen, in a sentence or two. */
  readonly message: string
  /** The confirming button's label, e.g. "Ban member". */
  readonly confirm: string
  /** Whether the action cannot be taken back; styles the button. */
  readonly danger: boolean
  /** Whether a reason is asked for, and whether it may be left empty. */
  readonly reason: "none" | "optional" | "required"
  /** Called with the reason when the person confirms. */
  readonly run: (reason: string) => void
}

/**
 * A modal that asks before something is done. Set `request` to open it; it
 * closes itself and announces `closed` when the person confirms or cancels.
 * It is a native <dialog>, so focus is trapped and Escape cancels.
 */
@customElement("op-confirm")
export class OpConfirm extends LitElement {
  static override styles = [base, controls, dialogs]

  @property({ attribute: false }) request: Option.Option<ConfirmRequest> = Option.none()

  @query("dialog") private dialog!: HTMLDialogElement

  protected override updated(changed: PropertyValues<this>): void {
    if (!changed.has("request")) return
    if (Option.isSome(this.request) && !this.dialog.open) this.dialog.showModal()
    if (Option.isNone(this.request) && this.dialog.open) this.dialog.close()
  }

  override render() {
    return html`
      <dialog @close=${this.#closed} aria-labelledby="heading">
        ${Option.match(this.request, {
          onNone: () => nothing,
          onSome: (request) => html`
            <form method="dialog" class="stack" @submit=${(event: SubmitEvent) => this.#submit(event, request)}>
              <h2 id="heading">${request.heading}</h2>
              <p>${request.message}</p>
              ${request.reason === "none"
                ? nothing
                : html`
                    <label>
                      Reason ${request.reason === "optional" ? html`<span class="hint">optional</span>` : nothing}
                      <textarea name="reason" ?required=${request.reason === "required"} autofocus></textarea>
                      <span class="hint">It is recorded, and the person affected can read it.</span>
                    </label>
                  `}
              <div class="row">
                <button type="submit" class=${request.danger ? "danger" : "primary"} value="confirm">${request.confirm}</button>
                <button type="button" @click=${() => this.dialog.close()}>Cancel</button>
              </div>
            </form>
          `,
        })}
      </dialog>
    `
  }

  #submit(event: SubmitEvent, request: ConfirmRequest): void {
    const form = event.target as HTMLFormElement
    const reason = new FormData(form).get("reason")
    request.run(typeof reason === "string" ? reason.trim() : "")
  }

  readonly #closed = (): void => {
    this.request = Option.none()
    this.dispatchEvent(new CustomEvent("closed", { bubbles: true, composed: true }))
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "op-confirm": OpConfirm
  }
  interface HTMLElementEventMap {
    closed: CustomEvent<void>
  }
}
