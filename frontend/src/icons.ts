/**
 * The icons the palette and the canvas draw.
 *
 * Imported by name rather than through lucide's barrel, which pulls in every
 * one of its ~1500 icons and cost 800KB of the bundle when it was doing that.
 *
 * This is the one list in the frontend that tracks the registry, and it is
 * deliberately not load-bearing: an icon that is not here falls back to a
 * generic box, so adding a component with a new icon degrades rather than
 * breaks. Regenerate the list with:
 *
 *   ./target/debug/etl.exe components --manifest |
 *     python -c "import sys,json; m=json.load(sys.stdin); print(sorted({c['icon'] for c in m['components'] if c.get('icon')}))"
 */

import {
  Box,
  ArrowDownUp,
  CircleDot,
  CircleMinus,
  Clock,
  CloudDownload,
  CloudUpload,
  Code,
  Columns3,
  CopyCheck,
  CopyMinus,
  Database,
  Dices,
  FileBox,
  FileJson,
  FileText,
  Filter,
  Fingerprint,
  GitBranch,
  GitMerge,
  Globe,
  Hash,
  Link,
  ListChecks,
  ListEnd,
  ListOrdered,
  MountainSnow,
  OctagonX,
  PanelTop,
  Pencil,
  Regex,
  Rows3,
  Ruler,
  ScrollText,
  Sheet,
  ShieldAlert,
  Sigma,
  SquarePlus,
  Table2,
  TableColumnsSplit,
  TableProperties,
  Triangle,
  Type,
  type LucideIcon,
} from "lucide-react";

/** Spec icon name to component. Keys are the kebab-case names specs carry. */
const BY_NAME: Record<string, LucideIcon> = {
  "arrow-down-up": ArrowDownUp,
  "circle-dot": CircleDot,
  "circle-minus": CircleMinus,
  "clock": Clock,
  "cloud-download": CloudDownload,
  "cloud-upload": CloudUpload,
  "code": Code,
  "columns-3": Columns3,
  "copy-check": CopyCheck,
  "copy-minus": CopyMinus,
  "database": Database,
  "dices": Dices,
  "file-box": FileBox,
  "file-json": FileJson,
  "file-text": FileText,
  "filter": Filter,
  "fingerprint": Fingerprint,
  "git-branch": GitBranch,
  "git-merge": GitMerge,
  "globe": Globe,
  "hash": Hash,
  "link": Link,
  "list-checks": ListChecks,
  "list-end": ListEnd,
  "list-ordered": ListOrdered,
  "mountain-snow": MountainSnow,
  "octagon-x": OctagonX,
  "panel-top": PanelTop,
  "pencil": Pencil,
  "regex": Regex,
  "rows-3": Rows3,
  "ruler": Ruler,
  "scroll-text": ScrollText,
  "sheet": Sheet,
  "shield-alert": ShieldAlert,
  "sigma": Sigma,
  "square-plus": SquarePlus,
  "table-2": Table2,
  "table-columns-split": TableColumnsSplit,
  "table-properties": TableProperties,
  "triangle": Triangle,
  "type": Type,
};

/** The icon for a spec, or a generic box when the name is new to us. */
export function iconFor(name: string | undefined): LucideIcon {
  return (name && BY_NAME[name]) || Box;
}
