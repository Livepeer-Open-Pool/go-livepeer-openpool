import "@openpool/ui/global.css"
import { type InjectedProvider, makeBrowserRuntime } from "@openpool/core"
import { Option, Predicate } from "effect"
import { MemberApp } from "./member-app.js"

// The site's one entry point: it builds the runtime from what the browser
// offers and hands it to the root element. Nothing else runs effects on its own.

const injected: unknown = Reflect.get(window, "ethereum")
const wallet: Option.Option<InjectedProvider> =
  Predicate.hasProperty(injected, "request") && Predicate.isFunction(injected.request)
    ? Option.some(injected as InjectedProvider)
    : Option.none()

MemberApp.runtime = makeBrowserRuntime({
  configUrl: new URL("config.json", document.baseURI).href,
  location: { host: location.host, origin: location.origin },
  storage: localStorage,
  wallet,
})

customElements.define("member-app", MemberApp)
