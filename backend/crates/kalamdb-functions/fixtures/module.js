function kalamInvoke(name, args) {
  switch (name) {
    case "echo":
      return args[0];
    case "hang":
      for (;;) {}
    case "oom": {
      const chunks = [];
      // Grow in small steps so V8's near-heap-limit callback can fire
      // before a single allocation trips FatalProcessOutOfMemory.
      for (;;) {
        chunks.push("x".repeat(4096));
      }
    }
    default:
      throw new Error("unknown procedure: " + name);
  }
}
