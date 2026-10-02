import { type Address, formatEth, type Price, type Wei } from "@openpool/core"
import { DateTime } from "effect"
import { html, type TemplateResult } from "lit"

// How the sites write the pool's values. Each returns markup that carries
// the exact value for machines and people who want it: a time in <time>, an
// amount with its wei in a title.

const dateTime = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" })
const dateOnly = new Intl.DateTimeFormat(undefined, { dateStyle: "medium" })

/** A moment, in the reader's own time zone. */
export const time = (moment: DateTime.Utc): TemplateResult =>
  html`<time datetime=${DateTime.formatIso(moment)}>${dateTime.format(DateTime.toDateUtc(moment))}</time>`

/** A day, in the reader's own time zone. */
export const day = (moment: DateTime.Utc): TemplateResult =>
  html`<time datetime=${DateTime.formatIso(moment)}>${dateOnly.format(DateTime.toDateUtc(moment))}</time>`

/** An amount of ETH; the exact wei is one hover away. */
export const eth = (amount: Wei, digits = 6): TemplateResult =>
  html`<data class="num" value=${amount.toString()} title="${amount.toString()} wei">${formatEth(amount, digits)} ETH</data>`

/** 0x5aAe…eAed: enough of an address to recognize it, with the whole in a title. */
export const shortAddress = (address: Address | string): TemplateResult =>
  html`<code title=${address} translate="no">${address.slice(0, 6)}…${address.slice(-4)}</code>`

const unitLabel: Readonly<Record<Price["unit"], string>> = {
  hour: "per hour",
  "720p": "per 720p-equivalent hour",
  fixed: "per request",
}

/** "$0.50 per hour". */
export const formatPrice = (price: Price): string => `$${price.price} ${unitLabel[price.unit]}`

/** 24564 MB is "24 GB". */
export const formatMemory = (megabytes: number): string => `${Math.round(megabytes / 1024)} GB`

/** A count with its noun: "1 machine", "3 machines". */
export const plural = (count: number, one: string, many = `${one}s`): string => `${count} ${count === 1 ? one : many}`
