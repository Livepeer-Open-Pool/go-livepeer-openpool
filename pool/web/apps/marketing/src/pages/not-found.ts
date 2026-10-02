import { html } from "@lit-labs/ssr"
import type { Route } from "../routes.js"

export const notFoundPage: Route = {
  path: "/404.html",
  mode: "ssg",
  meta: () => ({
    title: "Page not found",
    description: "There is no page at this address on Open Pool's site.",
    path: "/404.html",
    index: false,
    jsonLd: [],
  }),
  template: () => html`
    <article class="prose">
      <h1>There is no page at this address</h1>
      <p>It may have moved, or the link may be mistyped.</p>
      <p class="actions"><a class="button primary" href="/">Go to the home page</a> <a class="button" href="/connect/">Connect a machine</a></p>
    </article>
  `,
}
