import "./marketing.css"
import { ContextProvider } from "@lit/context"
import { makeBrowserRuntime } from "@openpool/core"
import { runtimeContext } from "@openpool/ui"
import { Option } from "effect"

// What the browser runs on the public site. The pages themselves are plain
// HTML written at build time; this adds the parts that show live data from
// the pool's regions. It needs no wallet and keeps no session.

const runtime = makeBrowserRuntime({
  configUrl: new URL("/config.json", location.origin).href,
  location: { host: location.host, origin: location.origin },
  storage: sessionStorage,
  wallet: Option.none(),
})

// The live parts find the runtime through the page's body, wherever they are.
new ContextProvider(document.body, { context: runtimeContext, initialValue: runtime })

// Defined after the runtime is in place, so each one finds it when it starts.
void Promise.all([
  import("./islands/op-pool-facts.js"),
  import("./islands/op-live-apps.js"),
  import("./islands/op-leaderboard.js"),
  import("./islands/op-live-terms.js"),
  import("./islands/op-connect-command.js"),
  import("./islands/op-region-status.js"),
])
