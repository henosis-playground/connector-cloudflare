const reference = (kind, component, output) =>
  Object.freeze({ kind, component, output });

export const input = Object.freeze({
  string: (component, output) => reference("string", component, output),
  url: (component, output) => reference("url", component, output),
  secret: (component, output) => reference("secret", component, output),
});

export const defineWorker = (definition) => Object.freeze(definition);
