import { clsx } from 'clsx'

type Phase =
  | 'Pending'
  | 'Scheduling'
  | 'Running'
  | 'Succeeded'
  | 'Failed'
  | 'Degraded'

const styles: Record<Phase, string> = {
  Pending:    'bg-gray-700 text-gray-300',
  Scheduling: 'bg-blue-900 text-blue-300',
  Running:    'bg-yellow-900 text-yellow-300',
  Succeeded:  'bg-green-900 text-green-300',
  Failed:     'bg-red-900 text-red-300',
  Degraded:   'bg-orange-900 text-orange-300',
}

interface Props {
  phase: string
}

export default function StatusBadge({ phase }: Props) {
  const style = styles[phase as Phase] ?? 'bg-gray-700 text-gray-300'
  return (
    <span className={clsx('inline-block px-2 py-0.5 rounded text-xs font-semibold', style)}>
      {phase}
    </span>
  )
}
