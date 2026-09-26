import { Link } from 'react-router-dom';
import { ChevronRight, Home } from 'lucide-react';

export interface Crumb { label: string; url: string }

export function Breadcrumb({ items }: { items: Crumb[] }) {
  if (!items.length) return null;
  return (
    <nav className="flex flex-wrap items-center gap-1 border-b border-slate-200 bg-white px-6 py-2 text-sm text-slate-500">
      {items.map((item, i) => (
        <span key={item.url + i} className="flex items-center gap-1">
          {i > 0 && <ChevronRight size={14} />}
          {i === 0 && <Home size={14} />}
          {i < items.length - 1
            ? <Link to={item.url} className="hover:text-indigo-600">{item.label}</Link>
            : <span className="font-medium text-slate-900">{item.label}</span>}
        </span>
      ))}
    </nav>
  );
}
