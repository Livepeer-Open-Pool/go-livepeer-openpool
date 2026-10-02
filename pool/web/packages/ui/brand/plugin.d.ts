import type { Plugin } from "vite"

/** What a site says about itself in its web app manifest. */
export interface BrandSite {
  /** The site's full name, e.g. "Open Pool members". */
  readonly name: string
  /** A name short enough for a home screen. Defaults to the full name. */
  readonly shortName?: string
  readonly description: string
  /** How the site opens once installed. Defaults to "standalone". */
  readonly display?: "standalone" | "minimal-ui" | "browser"
  /** False for a site that writes its own pages and puts headTags in them itself. */
  readonly head?: boolean
}

/** One element of a page's head. */
export interface HeadTag {
  readonly tag: "link" | "meta" | "script"
  readonly attrs: Readonly<Record<string, string>>
  /** A script's own text. */
  readonly children?: string
}

/** The ground of each theme, for the browser's own chrome. */
export declare const themeColors: { readonly dark: string; readonly light: string }

/** The script that applies the visitor's theme before the first paint. */
export declare const themeScript: string

/** What every site's head says about its icons, manifest and theme. */
export declare const headTags: ReadonlyArray<HeadTag>

/** A site's web app manifest. */
export declare const manifest: (site: BrandSite) => Readonly<Record<string, unknown>>

/** Serves the logo, icons and manifest from a site's root, and writes them into its build. */
export declare const brand: (site: BrandSite) => Plugin
