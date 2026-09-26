import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { KeyRound } from 'lucide-react';
import { getToken, setToken } from '@/lib/api';

/** Where the access token goes. The service derives the actor from it, so this
 *  is the only place identity enters the app — no screen asks who you are.
 *
 *  A deployment with an identity provider replaces this with the
 *  authorization-code flow; the rest of the app is unchanged, because it only
 *  ever reads `getToken()`. */
export function TokenBar() {
  const [value, setValue] = useState(getToken() ?? '');
  const [open, setOpen] = useState(!getToken());
  const client = useQueryClient();

  function save() {
    setToken(value.trim() || null);
    client.invalidateQueries();
    setOpen(false);
  }

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="flex items-center gap-1.5 text-sm text-indigo-100 hover:text-white"
        title="Change the access token"
      ><KeyRound size={14} /> token</button>
    );
  }
  return (
    <div className="flex items-center gap-2">
      <input
        value={value}
        onChange={e => setValue(e.target.value)}
        onKeyDown={e => e.key === 'Enter' && save()}
        placeholder="bearer token"
        className="w-56 rounded border border-indigo-400 bg-indigo-800 px-2 py-1 text-sm text-white placeholder:text-indigo-300"
      />
      <button onClick={save} className="rounded bg-white px-2 py-1 text-sm text-indigo-700">use</button>
    </div>
  );
}
