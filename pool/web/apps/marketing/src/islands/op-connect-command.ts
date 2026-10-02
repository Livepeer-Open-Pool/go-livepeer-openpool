import { answered, Api, everyRegion } from "@openpool/core"
import { regionNotices, renderQuery, SitePage } from "@openpool/ui"
import { Option } from "effect"
import { html } from "lit"
import { customElement } from "lit/decorators.js"

/** The install command of each region, with the region's real address, agent image and signing key. */
@customElement("op-connect-command")
export class OpConnectCommand extends SitePage {
  readonly #pools = this.query(() => everyRegion(Api.pool))

  override render() {
    return renderQuery(this.#pools, (results) => html`
      <div class="stack-s">
        ${regionNotices(results)}
        ${answered(results).map(({ region, value }) =>
          Option.match(value.agent, {
            onNone: () => html`<p class="muted">${region.name} does not use the agent: its members start their runners by hand, with the commands on their machines page.</p>`,
            onSome: (agent) => html`
              <op-copy
                label="${region.name}: the command, without your credential"
                .text=${[
                  `docker run -d --name openpool-agent-${region.id} --restart unless-stopped --gpus all \\`,
                  `  -v /var/run/docker.sock:/var/run/docker.sock \\`,
                  `  -e OPENPOOL_CREDENTIAL=<shown once on your machines page> \\`,
                  `  ${agent.image} --pool ${region.api} --key ${agent.key}`,
                ].join("\n")}
              ></op-copy>
            `,
          }),
        )}
        <p class="muted small">The key is what the agent checks the pool's signature against. It should match the one in the command on your machines page.</p>
      </div>
    `)
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "op-connect-command": OpConnectCommand
  }
}
