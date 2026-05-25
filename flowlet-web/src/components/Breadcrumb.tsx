import { Link } from 'react-router-dom';
import { ChevronRight, Home } from 'lucide-react';

export interface BreadcrumbItem { label: string; url: string; }

export function Breadcrumb({ items }: { items: BreadcrumbItem[] }) {
  if (!items.length) return null;
  return (
    <nav className="flex items-center gap-1 px-6 py-2 bg-gray-100 text-sm text-gray-600 flex-wrap">
      {items.map((item, i) => (
        <span key={i} className="flex items-center gap-1">
          {i > 0 && <ChevronRight size={14} />}
          {i === 0 && <Home size={14} />}
          {i < items.length - 1
            ? <Link to={item.url} className="hover:text-blue-600">{item.label}</Link>
            : <span className="text-gray-900 font-medium">{item.label}</span>}
        </span>
      ))}
    </nav>
  );
}
