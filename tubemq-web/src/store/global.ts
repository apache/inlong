import { createStore } from '@reactseed/use-redux';

export interface TState {
  name: string;
  age: number;
}

export interface TMethod {
  updateName: (name: string) => void;
  becomeOlder: () => void;
}

const store = createStore(() => ({
  age: 20,
  name: 'reactseed',
}));

const methods = (state: TState): TMethod => {
  const { age } = state;
  return {
    updateName: (name: string) => {
      state.name = name;
    },
    becomeOlder: () => {
      state.age = age + 1;
    },
  };
};

export { store, methods };
