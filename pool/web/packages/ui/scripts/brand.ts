import { readFile, writeFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"
import sharp from "sharp"

// Draws everything that is made from the logo: the icons browsers and phones
// ask for, and the stylesheet that lets a page paint the mark in its theme's
// colors. The one source is brand/mark.svg; what this writes is committed, so
// building a site needs none of it. Run it after changing the mark:
//
//   pnpm --filter @openpool/ui brand

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")
const brand = path.join(root, "brand")

/** The ground the mark sits on wherever an icon needs one: the dark theme's surface. */
const ground = "#050b1a"

const mark = await readFile(path.join(brand, "mark.svg"), "utf8")
const inner = mark.replace(/^[\s\S]*?<svg[^>]*>/, "").replace(/<\/svg>\s*$/, "").replace(/<title>[\s\S]*?<\/title>/, "")

/** A square icon: the mark at a share of the side, on the ground, with corners rounded by a share of the side. */
const icon = (side: number, share: number, corner: number): Promise<Buffer> => {
  const size = side * share
  const offset = (side - size) / 2
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${side}" height="${side}" viewBox="0 0 ${side} ${side}">
    <rect width="${side}" height="${side}" rx="${side * corner}" fill="${ground}"/>
    <svg x="${offset}" y="${offset}" width="${size}" height="${size}" viewBox="0 0 100 100">${inner}</svg>
  </svg>`
  return sharp(Buffer.from(svg)).png({ compressionLevel: 9 }).toBuffer()
}

/** The mark alone, on nothing, for the small sizes a browser tab shows. */
const bare = (side: number): Promise<Buffer> => sharp(Buffer.from(mark), { density: 72 * (side / 100) * 4 }).resize(side, side).png({ compressionLevel: 9 }).toBuffer()

/** An .ico file holding PNG images, which every browser that still asks for /favicon.ico reads. */
const ico = (images: ReadonlyArray<{ readonly side: number; readonly png: Buffer }>): Buffer => {
  const header = Buffer.alloc(6)
  header.writeUInt16LE(1, 2)
  header.writeUInt16LE(images.length, 4)
  const start = 6 + 16 * images.length
  const entries = images.map((image, index) => {
    const entry = Buffer.alloc(16)
    entry.writeUInt8(image.side === 256 ? 0 : image.side, 0)
    entry.writeUInt8(image.side === 256 ? 0 : image.side, 1)
    entry.writeUInt16LE(1, 4)
    entry.writeUInt16LE(32, 6)
    entry.writeUInt32LE(image.png.length, 8)
    entry.writeUInt32LE(start + images.slice(0, index).reduce((sum, earlier) => sum + earlier.png.length, 0), 12)
    return entry
  })
  return Buffer.concat([header, ...entries, ...images.map((image) => image.png)])
}

// A home-screen icon is cropped by the phone, so it fills its square; a
// maskable one keeps the mark inside the middle the crop never reaches.
await writeFile(path.join(brand, "apple-touch-icon.png"), await icon(180, 0.66, 0))
await writeFile(path.join(brand, "icon-192.png"), await icon(192, 0.68, 0.22))
await writeFile(path.join(brand, "icon-512.png"), await icon(512, 0.68, 0.22))
await writeFile(path.join(brand, "icon-maskable-512.png"), await icon(512, 0.54, 0))
await writeFile(
  path.join(brand, "favicon.ico"),
  ico(await Promise.all([16, 32, 48].map((side) => bare(side).then((png) => ({ side, png }))))),
)

// The mark's shape alone, as a mask: a page fills it with its own gradient,
// so one shape serves the dark theme and the light one.
const shape = mark
  .replace(/<title>[\s\S]*?<\/title>/, "")
  .replace(/<linearGradient[\s\S]*?<\/linearGradient>/, "")
  .replace('fill="url(#fill)"', 'fill="#000"')
  .replace(/>\s+</g, "><")
  .trim()
const dataUri = `data:image/svg+xml,${shape.replace(/"/g, "'").replace(/#/g, "%23").replace(/</g, "%3C").replace(/>/g, "%3E")}`
await writeFile(
  path.join(root, "src/brand.css"),
  `/* Written by scripts/brand.ts from brand/mark.svg. Do not edit; run "pnpm --filter @openpool/ui brand". */
@layer tokens {
  :root {
    --mark-shape: url("${dataUri}");
  }
}
`,
)

console.log("wrote the icons to brand/ and the mark's shape to src/brand.css")
