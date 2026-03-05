export async function flagItem(id: number, flagged: boolean): Promise<void> {
  const res = await fetch(`/api/items/${id}/flag`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ flagged_for_deletion: flagged }),
  })
  if (!res.ok) {
    throw new Error(`Failed to update flag: ${res.status}`)
  }
}

export async function editItem(
  id: number,
  updates: { title?: string; tags?: string[] },
): Promise<void> {
  const res = await fetch(`/api/items/${id}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(updates),
  })
  if (!res.ok) {
    throw new Error(`Failed to update item: ${res.status}`)
  }
}
