export type OwnedClose = { name: string; close: () => Promise<void> };
export async function closeOwned(owned: OwnedClose[]): Promise<string[]> {
  const errors: string[] = [];
  for (const resource of [...owned].reverse()) {
    try {
      await resource.close();
    } catch (error) {
      errors.push(`${resource.name}: ${String(error)}`);
    }
  }
  return errors;
}

// Only a fulfilled close acknowledges release; a rejected attempt remains owned.
export function acknowledgedClose(
  name: string,
  close: () => Promise<void>,
): OwnedClose {
  let acknowledged = false;
  return {
    name,
    close: async () => {
      if (acknowledged) return;
      await close();
      acknowledged = true;
    },
  };
}
