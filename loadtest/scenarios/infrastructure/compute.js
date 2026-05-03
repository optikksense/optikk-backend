// Composite scenario: walks cpu + memory + jvm submodules per iteration.
// Per-submodule scenarios are defined in cpu.js / memory.js / jvm.js so they
// can be exercised standalone via entrypoints/granular.js.

import { infraCPU }    from './cpu.js';
import { infraMemory } from './memory.js';
import { infraJVM }    from './jvm.js';

export function infraCompute(ctx) {
  infraCPU(ctx);
  infraMemory(ctx);
  infraJVM(ctx);
}
