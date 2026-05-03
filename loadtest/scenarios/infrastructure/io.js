// Composite scenario: walks disk + network + connpool submodules per iteration.
// Per-submodule scenarios are defined in disk.js / network.js / connpool.js so
// they can be exercised standalone via entrypoints/granular.js.

import { infraDisk }     from './disk.js';
import { infraNetwork }  from './network.js';
import { infraConnPool } from './connpool.js';

export function infraIO(ctx) {
  infraDisk(ctx);
  infraNetwork(ctx);
  infraConnPool(ctx);
}
