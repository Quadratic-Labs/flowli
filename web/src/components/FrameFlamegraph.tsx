import { useRef, useEffect } from 'react';
import * as d3 from 'd3';
import type { FrameNode } from '@/lib/api';
import { humanizeDuration, statusColor } from '@/lib/format';

/** A frame laid out in time. Depth comes from the frame id, which is a path
 *  (spec 01 section 3.1) — no span hierarchy is reconstructed here, because
 *  the journal has the tree already. */
interface Bar {
  fid: string; name: string; kind: string; status: string;
  start: number; end: number; depth: number; attempts: number; suspended: boolean;
}

function toBars(node: FrameNode, depth: number, now: number, out: Bar[] = []): Bar[] {
  const start = node.started_at ? new Date(node.started_at).getTime() : now;
  const end = node.ended_at ? new Date(node.ended_at).getTime() : now;
  out.push({
    fid: node.fid, name: node.name, kind: node.kind, status: node.status,
    start, end: Math.max(end, start), depth, attempts: node.attempts,
    suspended: node.status === 'suspended',
  });
  for (const child of node.children) toBars(child, depth + 1, now, out);
  return out;
}

const MARGIN = { top: 16, right: 16, bottom: 36, left: 48 };
const ROW = 26;
const MOVE_MS = 350;

/** Draws the frame tree as a flamegraph, updating in place: the scaffold is
 *  built once and data flows through a keyed join on the frame id, so a poll
 *  that changes nothing redraws nothing. A suspended frame is hatched: it is
 *  waiting, not working. */
export function FrameFlamegraph({ root, onSelect }: {
  root: FrameNode;
  onSelect?: (fid: string) => void;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const tooltipRef = useRef<d3.Selection<HTMLDivElement, unknown, HTMLElement, unknown> | null>(null);

  useEffect(() => {
    tooltipRef.current = d3.select('body').append('div')
      .attr('class', 'flamegraph-tip')
      .style('position', 'absolute').style('visibility', 'hidden')
      .style('background', 'rgba(15,23,42,0.94)').style('color', '#fff')
      .style('padding', '6px 8px').style('border-radius', '6px')
      .style('font-size', '12px').style('line-height', '1.4')
      .style('pointer-events', 'none').style('z-index', '9999');
    return () => { tooltipRef.current?.remove(); tooltipRef.current = null; };
  }, []);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const bars = toBars(root, 0, Date.now());
    const depth = Math.max(...bars.map(b => b.depth)) + 1;
    const height = depth * ROW;
    const width = Math.max(320, container.offsetWidth) - MARGIN.left - MARGIN.right;

    let svg = d3.select(container).select<SVGSVGElement>('svg');
    if (svg.empty()) {
      svg = d3.select(container).append('svg');
      const defs = svg.append('defs');
      // One hatch pattern, reused: a suspended frame is waiting, not working.
      const hatch = defs.append('pattern').attr('id', 'frame-wait')
        .attr('width', 6).attr('height', 6).attr('patternUnits', 'userSpaceOnUse')
        .attr('patternTransform', 'rotate(45)');
      hatch.append('rect').attr('width', 6).attr('height', 6).attr('fill', statusColor('suspended'));
      hatch.append('rect').attr('width', 2).attr('height', 6).attr('fill', 'rgba(255,255,255,0.45)');
      const g = svg.append('g').attr('class', 'chart')
        .attr('transform', `translate(${MARGIN.left},${MARGIN.top})`);
      g.append('g').attr('class', 'bars');
      g.append('g').attr('class', 'x-axis');
    }
    svg.attr('width', width + MARGIN.left + MARGIN.right)
      .attr('height', height + MARGIN.top + MARGIN.bottom);

    const t0 = Math.min(...bars.map(b => b.start));
    const t1 = Math.max(...bars.map(b => b.end));
    const span = Math.max(1, t1 - t0);
    const x = d3.scaleLinear().domain([0, span]).range([0, width]);

    const barX = (b: Bar) => x(b.start - t0);
    const barY = (b: Bar) => b.depth * ROW;
    const barW = (b: Bar) => Math.max(2, x(Math.max(b.end - b.start, 0)));
    const fill = (b: Bar) => (b.suspended ? 'url(#frame-wait)' : statusColor(b.status));

    const join = svg.select<SVGGElement>('.bars')
      .selectAll<SVGGElement, Bar>('g.bar')
      .data(bars, b => b.fid)
      .join(
        enter => {
          const g = enter.append('g').attr('class', 'bar').style('cursor', 'pointer');
          g.append('rect')
            .attr('x', barX).attr('y', barY).attr('width', 0).attr('height', ROW - 4)
            .attr('rx', 3).attr('fill', fill)
            .on('mouseover', function (_e, b) {
              d3.select(this).attr('opacity', 0.78);
              tooltipRef.current?.style('visibility', 'visible').html(
                `<strong>${b.name}</strong> <span style="opacity:.7">${b.kind}</span><br/>` +
                `${b.status}${b.attempts > 1 ? ` · ${b.attempts} attempts` : ''}<br/>` +
                `${humanizeDuration(b.end - b.start)}<br/>` +
                `<span style="opacity:.7">${b.fid}</span>`,
              );
            })
            .on('mousemove', (event: MouseEvent) => {
              tooltipRef.current
                ?.style('top', `${event.pageY - 12}px`)
                .style('left', `${event.pageX + 12}px`);
            })
            .on('mouseout', function () {
              d3.select(this).attr('opacity', 1);
              tooltipRef.current?.style('visibility', 'hidden');
            })
            .on('click', (_e, b) => onSelect?.(b.fid));
          g.append('text')
            .attr('x', b => barX(b) + 6).attr('y', b => barY(b) + (ROW - 4) / 2)
            .attr('dy', '0.35em').attr('fill', '#fff').attr('font-size', '11px')
            .style('pointer-events', 'none');
          return g;
        },
        update => update,
        exit => exit.transition().duration(MOVE_MS).style('opacity', 0).remove(),
      );

    join.select<SVGRectElement>('rect').transition().duration(MOVE_MS)
      .attr('x', barX).attr('y', barY).attr('width', barW).attr('height', ROW - 4)
      .attr('fill', fill);
    join.select<SVGTextElement>('text')
      .text(b => (barW(b) > 46 ? b.name : ''))
      .transition().duration(MOVE_MS)
      .attr('x', b => barX(b) + 6).attr('y', b => barY(b) + (ROW - 4) / 2);

    svg.select<SVGGElement>('.x-axis')
      .attr('transform', `translate(0,${height})`)
      .transition().duration(MOVE_MS)
      .call(d3.axisBottom(x).ticks(6).tickFormat(d => humanizeDuration(Number(d))) as never);
  }, [root, onSelect]);

  return <div ref={containerRef} className="w-full overflow-x-auto" />;
}
