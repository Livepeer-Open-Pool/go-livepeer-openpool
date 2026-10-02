import "@openpool/ui/global.css"
import { makeBrowserRuntime } from "@openpool/core"
import { Option } from "effect"
import { AdminApp } from "./admin-app.js"

// The site's one entry point. The operator signs in with an emailed code, so
// this site never asks for a wallet.
AdminApp.runtime = makeBrowserRuntime({
  configUrl: new URL("config.json", document.baseURI).href,
  location: { host: location.host, origin: location.origin },
  storage: localStorage,
  wallet: Option.none(),
})

customElements.define("admin-app", AdminApp)
