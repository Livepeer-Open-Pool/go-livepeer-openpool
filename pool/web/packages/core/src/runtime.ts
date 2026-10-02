import { FetchHttpClient } from "@effect/platform"
import { Layer, ManagedRuntime, Option } from "effect"
import type { ConfigUnavailable } from "./domain/errors.js"
import { PoolApi, PoolApiLive } from "./services/PoolApi.js"
import { SessionStore, SessionStoreBrowser } from "./services/SessionStore.js"
import { SiteSettings, SiteSettingsFromFile } from "./services/SiteSettings.js"
import { type InjectedProvider, Wallet, WalletInjected } from "./services/Wallet.js"

/** Everything a site's workflows can ask for. */
export type SiteServices = PoolApi | SessionStore | SiteSettings | Wallet

/** A site's one runtime: every effect the site runs goes through it. */
export type SiteRuntime = ManagedRuntime.ManagedRuntime<SiteServices, ConfigUnavailable>

/** What the browser gives a site. */
export interface BrowserEnvironment {
  /** Where the site's settings file is, e.g. /config.json. */
  readonly configUrl: string
  readonly location: { readonly host: string; readonly origin: string }
  readonly storage: Storage
  readonly wallet: Option.Option<InjectedProvider>
}

/** The services of a site running in a browser. */
export const browserLayer = (environment: BrowserEnvironment): Layer.Layer<SiteServices, ConfigUnavailable> =>
  Layer.mergeAll(
    PoolApiLive,
    SiteSettingsFromFile(environment.configUrl, environment.location),
    SessionStoreBrowser(environment.storage),
    WalletInjected(environment.wallet),
  ).pipe(Layer.provide(FetchHttpClient.layer))

/** The runtime of a site running in a browser. Make one, at the site's root. */
export const makeBrowserRuntime = (environment: BrowserEnvironment): SiteRuntime => ManagedRuntime.make(browserLayer(environment))
