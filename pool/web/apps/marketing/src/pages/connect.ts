import { html } from "@lit-labs/ssr"
import { breadcrumb, type Route } from "../routes.js"

export const connectPage: Route = {
  path: "/connect/",
  mode: "ssg",
  meta: (site) => ({
    title: "Connect a GPU machine",
    description:
      "Step-by-step: what a machine needs to join Open Pool, the one docker command that connects it, what the agent does, and how to check, update or remove it.",
    path: "/connect/",
    index: true,
    jsonLd: [breadcrumb(site, "Connect a machine", "/connect/")],
  }),
  template: (site) => html`
    <article class="prose">
      <header>
        <h1>Connect a GPU machine</h1>
        <p class="lede">One command per machine. After that, the machine runs whatever the pool assigns it and keeps itself up to date.</p>
      </header>

      <section aria-labelledby="before">
        <h2 id="before">Before you start</h2>
        <p>The machine needs:</p>
        <ul>
          <li>Linux, on <abbr title="64-bit x86">x86-64</abbr>.</li>
          <li>An NVIDIA GPU of a model the region accepts, with a recent driver. Check with <kbd>nvidia-smi</kbd>.</li>
          <li><a href="https://docs.docker.com/engine/install/" rel="external">Docker Engine</a> with the Compose plugin.</li>
          <li>
            The <a href="https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html" rel="external">NVIDIA container toolkit</a>,
            so containers can use the GPU. Check with <kbd>docker run --rm --gpus all ubuntu nvidia-smi</kbd>: it should list your GPUs.
          </li>
          <li>Disk space for the models: tens of gigabytes per app.</li>
          <li>An outbound internet connection. No ports need opening.</li>
        </ul>
      </section>

      <section aria-labelledby="steps">
        <h2 id="steps">The steps</h2>
        <ol class="steps">
          <li>
            <h3>Apply, and wait for approval</h3>
            <p>
              On the <a href="${site.memberUrl}/apply">member site</a>, sign in with your wallet, pick a region, and list each machine with
              its GPUs. The operator approves the application and assigns each machine its apps. Your overview shows when.
            </p>
          </li>
          <li>
            <h3>Create the machine's credential</h3>
            <p>
              On your machines page, press <strong>Create credential</strong> for the machine. The credential is shown once, inside
              the command to run. Each machine has its own; creating a new one stops the old one working.
            </p>
          </li>
          <li>
            <h3>Run the command on the machine</h3>
            <p>It looks like this. Use the one from your machines page, which has your credential in it:</p>
            <op-connect-command>
              <pre><code translate="no">docker run -d --name openpool-agent-&lt;region&gt; --restart unless-stopped --gpus all \\
  -v /var/run/docker.sock:/var/run/docker.sock \\
  -e OPENPOOL_CREDENTIAL=&lt;shown once on your machines page&gt; \\
  &lt;the agent image&gt; --pool &lt;the region's address&gt; --key &lt;the region's signing key&gt;</code></pre>
            </op-connect-command>
            <p>With rootless Docker, mount the socket <code>DOCKER_HOST</code> names in place of <code>/var/run/docker.sock</code>.</p>
          </li>
          <li>
            <h3>Watch it come up</h3>
            <p>
              Within a minute your machines page shows what the agent found: its version, the GPUs, the containers. The first start
              downloads the app's image and model, which can take a while. When the app is up, its runner appears on your runners
              page on probation, is tested, and goes active.
            </p>
          </li>
        </ol>
      </section>

      <section aria-labelledby="agent">
        <h2 id="agent">What the agent does</h2>
        <p>Every half minute it does three things:</p>
        <ol>
          <li><strong>Asks</strong> the pool what this machine should run. The answer is a Docker Compose file, signed by the pool.</li>
          <li>
            <strong>Checks and applies</strong> it. A file not signed with the key the agent was started with is refused. So is a
            correctly signed file that asks for more than app containers: privileged mode, the host's network or processes, added
            capabilities, devices, published ports, or mounts of your disks. Then it runs <code>docker compose up</code>.
          </li>
          <li><strong>Reports</strong> which containers are running, the GPUs it found, and any error.</li>
        </ol>
        <p>
          If the pool cannot be reached, the agent keeps running what it was last told. If the pool stops accepting the machine's
          credential, because the machine was removed or the credential replaced, it stops the pool's containers after an hour.
        </p>
      </section>

      <section aria-labelledby="questions">
        <h2 id="questions">Common questions</h2>
        <details name="faq">
          <summary>How do I see what is happening on the machine?</summary>
          <p><kbd>docker logs openpool-agent-&lt;region&gt;</kbd> shows the agent's log. <kbd>docker ps</kbd> shows the app and tunnel containers it started.</p>
        </details>
        <details name="faq">
          <summary>How do I update an app?</summary>
          <p>You do not. When the operator releases a new version, machines are moved to it a few at a time, each after finishing the sessions it is serving.</p>
        </details>
        <details name="faq">
          <summary>How do I take a machine out of the pool?</summary>
          <p>
            Remove it on your machines page. Its runners finish their sessions and are removed, and the agent stops the pool's
            containers. Then <kbd>docker rm -f openpool-agent-&lt;region&gt;</kbd> removes the agent. Model files stay in Docker
            volumes until you delete them.
          </p>
        </details>
        <details name="faq">
          <summary>My runner is suspended. Why?</summary>
          <p>Your runners page says why, with the result of every test. A runner suspended for failing tests comes back by itself once it passes again.</p>
        </details>
        <details name="faq">
          <summary>Can one machine join two regions?</summary>
          <p>Yes: apply in both and run each region's command. The two agents keep their containers apart. They share the GPUs, so make sure the machine can serve what both regions assign it.</p>
        </details>
      </section>
    </article>
  `,
}
