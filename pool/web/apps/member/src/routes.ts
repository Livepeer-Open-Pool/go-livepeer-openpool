import type { NavItem, Route } from "@openpool/ui"
import { html } from "lit"

export const routes: ReadonlyArray<Route> = [
  { path: "/", title: "Overview", enter: () => import("./pages/overview-page.js"), template: () => html`<overview-page></overview-page>` },
  { path: "/apply", title: "Apply", enter: () => import("./pages/apply-page.js"), template: () => html`<apply-page></apply-page>` },
  { path: "/machines", title: "Machines", enter: () => import("./pages/machines-page.js"), template: () => html`<machines-page></machines-page>` },
  { path: "/runners", title: "Runners", enter: () => import("./pages/runners-page.js"), template: () => html`<runners-page></runners-page>` },
  { path: "/earnings", title: "Earnings", enter: () => import("./pages/earnings-page.js"), template: () => html`<earnings-page></earnings-page>` },
  { path: "/payouts", title: "Payouts", enter: () => import("./pages/payouts-page.js"), template: () => html`<payouts-page></payouts-page>` },
]

export const notFound: Route = {
  path: "*",
  title: "Page not found",
  enter: () => import("./pages/not-found-page.js"),
  template: () => html`<not-found-page></not-found-page>`,
}

export const nav: ReadonlyArray<NavItem> = [
  { href: "/", label: "Overview" },
  { href: "/machines", label: "Machines" },
  { href: "/runners", label: "Runners" },
  { href: "/earnings", label: "Earnings" },
  { href: "/payouts", label: "Payouts" },
  { href: "/apply", label: "Apply" },
]
