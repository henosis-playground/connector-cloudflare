export interface InputReference<Value> {
  readonly kind: "string" | "url" | "secret";
  readonly component: string;
  readonly output: string;
  readonly __value?: Value;
}

export interface WorkerDefinition {
  readonly inputs?: Readonly<Record<string, InputReference<unknown>>>;
}

export interface InputBuilder {
  string(component: string, output: string): InputReference<string>;
  url(component: string, output: string): InputReference<string>;
  secret(component: string, output: string): InputReference<string>;
}

export declare const input: InputBuilder;
export declare function defineWorker<const Definition extends WorkerDefinition>(
  definition: Definition,
): Readonly<Definition>;
