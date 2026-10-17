import { batch, observable, opaqueObject } from "@legendapp/state";
import {
  subscribeProjection,
  type ProjectionFrame,
  type ProjectionOptions,
} from "./contract";

/** Engine-fed Legend observable. Exposes tracking reads, not set/delete or sync plugins. */
export function createLegendProjection<T, C, R>(
  options: ProjectionOptions<T, C, R>,
) {
  const state = observable(
    opaqueObject<{ frame: ProjectionFrame<T> }>({
      frame: { phase: "loading" },
    }),
  );
  const session = subscribeProjection(options, (frame) => {
    batch(() => state.set(opaqueObject({ frame })));
  });
  // Bound read methods preserve Legend tracking while withholding writable handles.
  const view = Object.freeze({
    get: (): ProjectionFrame<T> => state.get().frame,
    peek: (): ProjectionFrame<T> => state.peek().frame,
    onChange: (listener: (frame: ProjectionFrame<T>) => void): (() => void) =>
      state.onChange(() => listener(state.peek().frame)),
  });
  return Object.freeze({ view, commit: session.commit, close: session.close });
}
