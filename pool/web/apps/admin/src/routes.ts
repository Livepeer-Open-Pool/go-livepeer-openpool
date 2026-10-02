import type { NavItem, Route } from "@openpool/ui"
import { html } from "lit"

export const routes: ReadonlyArray<Route> = [
  { path: "/", title: "Overview", enter: () => import("./pages/overview-page.js"), template: () => html`<overview-page></overview-page>` },
  { path: "/applications", title: "Applications", enter: () => import("./pages/applications-page.js"), template: () => html`<applications-page></applications-page>` },
  { path: "/members", title: "Members", enter: () => import("./pages/members-page.js"), template: () => html`<members-page></members-page>` },
  {
    path: "/members/:region/:id",
    title: "Member",
    enter: () => import("./pages/member-page.js"),
    template: (p) => html`<member-page region=${p["region"] ?? ""} member=${p["id"] ?? ""}></member-page>`,
  },
  { path: "/machines", title: "Machines", enter: () => import("./pages/machines-page.js"), template: () => html`<machines-page></machines-page>` },
  { path: "/gpus", title: "GPU models", enter: () => import("./pages/gpus-page.js"), template: () => html`<gpus-page></gpus-page>` },
  { path: "/apps", title: "Apps", enter: () => import("./pages/apps-page.js"), template: () => html`<apps-page></apps-page>` },
  {
    path: "/apps/:region/:id",
    title: "App",
    enter: () => import("./pages/app-page.js"),
    template: (p) => html`<app-page region=${p["region"] ?? ""} app=${p["id"] ?? ""}></app-page>`,
  },
  { path: "/runners", title: "Runners", enter: () => import("./pages/runners-page.js"), template: () => html`<runners-page></runners-page>` },
  { path: "/payouts", title: "Payouts", enter: () => import("./pages/payouts-page.js"), template: () => html`<payouts-page></payouts-page>` },
  {
    path: "/payouts/:region/:id",
    title: "Payout batch",
    enter: () => import("./pages/batch-page.js"),
    template: (p) => html`<batch-page region=${p["region"] ?? ""} batch=${p["id"] ?? ""}></batch-page>`,
  },
  { path: "/terms", title: "Terms", enter: () => import("./pages/terms-page.js"), template: () => html`<terms-page></terms-page>` },
  { path: "/settings", title: "Settings", enter: () => import("./pages/settings-page.js"), template: () => html`<settings-page></settings-page>` },
  { path: "/audit", title: "Audit log", enter: () => import("./pages/audit-page.js"), template: () => html`<audit-page></audit-page>` },
]

export const notFound: Route = {
  path: "*",
  title: "Page not found",
  enter: () => import("./pages/not-found-page.js"),
  template: () => html`<not-found-page></not-found-page>`,
}

export const nav: ReadonlyArray<NavItem> = [
  { href: "/", label: "Overview" },
  { href: "/applications", label: "Applications" },
  { href: "/members", label: "Members" },
  { href: "/machines", label: "Machines" },
  { href: "/apps", label: "Apps" },
  { href: "/gpus", label: "GPUs" },
  { href: "/runners", label: "Runners" },
  { href: "/payouts", label: "Payouts" },
  { href: "/terms", label: "Terms" },
  { href: "/settings", label: "Settings" },
  { href: "/audit", label: "Audit" },
]
