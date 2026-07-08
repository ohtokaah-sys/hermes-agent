import { useMemo, useState } from 'react'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { BarChart3, ExternalLink } from '@/lib/icons'
import { cn } from '@/lib/utils'

import { ListRow, Pill, SectionHeading, SettingsContent } from '../primitives'

import type { BillingStateResponse } from './types'
import {
  type BillingAccountRowView,
  type BillingNoticeView,
  type BillingUsageRowView,
  deriveBillingView,
  EMPTY_BILLING_VALUE,
  useBillingState,
  useSubscriptionState
} from './use-billing-state'
import { useChargeFlow } from './use-charge-poller'

const FEATURE_BILLING_INVOICES = false

function openExternal(url?: string) {
  if (!url) {
    return
  }

  void window.hermesDesktop?.openExternal?.(url)
}

function SummaryCard({ label, value, tone }: { label: string; tone?: 'muted' | 'primary'; value: string }) {
  const pill = tone && value !== EMPTY_BILLING_VALUE

  return (
    <div className="min-w-0">
      <div className="text-[length:var(--conversation-caption-font-size)] text-(--ui-text-tertiary)">{label}</div>
      <div className="mt-1 flex min-w-0 items-center gap-2 text-lg font-semibold text-foreground">
        {pill ? <Pill tone={tone}>{value}</Pill> : <span className="truncate">{value}</span>}
      </div>
    </div>
  )
}

function NoticeCard({ notice }: { notice: BillingNoticeView }) {
  return (
    <div className="mb-5 rounded-lg border border-border/70 bg-muted/20 p-4">
      <div className="text-[length:var(--conversation-text-font-size)] font-medium text-foreground">{notice.title}</div>
      <div className="mt-1 text-[length:var(--conversation-caption-font-size)] leading-(--conversation-caption-line-height) text-(--ui-text-tertiary)">
        {notice.message}
      </div>
      {notice.action && (
        <Button
          className="mt-3"
          onClick={() => openExternal(notice.action?.url)}
          size="sm"
          type="button"
          variant="outline"
        >
          {notice.action.label}
          <ExternalLink className="size-3.5" />
        </Button>
      )}
    </div>
  )
}

function RowValue({ row }: { row: BillingAccountRowView }) {
  return (
    <div className="flex min-w-0 flex-wrap items-center justify-start gap-2 @2xl:justify-end">
      {row.value && (
        <span className="min-w-0 truncate text-[length:var(--conversation-text-font-size)] font-medium text-foreground">
          {row.value}
        </span>
      )}
      {row.pill && <Pill tone={row.pill.tone}>{row.pill.label}</Pill>}
      {row.secondaryPill && <Pill>{row.secondaryPill}</Pill>}
      {row.chips?.map(chip => (
        <Button disabled={chip.disabled} key={chip.label} size="sm" type="button" variant="outline">
          {chip.label}
        </Button>
      ))}
      {row.action && (
        <Button
          disabled={row.action.disabled}
          onClick={row.action.disabled ? undefined : () => openExternal(row.action?.url)}
          size="sm"
          type="button"
          variant="outline"
        >
          {row.action.label}
          {!row.action.disabled && row.action.url && <ExternalLink className="size-3.5" />}
        </Button>
      )}
    </div>
  )
}

function AccountRow({ billing, row }: { billing?: BillingStateResponse; row: BillingAccountRowView }) {
  if (row.id === 'buy_credits' && row.action && row.chips && billing?.can_charge && billing.cli_billing_enabled) {
    return <BuyCreditsRow billing={billing} row={row} />
  }

  return (
    <ListRow
      action={<RowValue row={row} />}
      below={
        row.caption ? (
          <div className="mt-1 text-[length:var(--conversation-caption-font-size)] text-(--ui-text-tertiary)">
            {row.caption}
          </div>
        ) : undefined
      }
      description={row.description}
      key={row.id}
      title={row.title}
    />
  )
}

function BuyCreditsRow({ billing, row }: { billing: BillingStateResponse; row: BillingAccountRowView }) {
  const presets = useMemo(
    () =>
      billing.charge_presets.map((amount, index) => ({
        amount,
        label: billing.charge_presets_display[index] || formatMoney(amount)
      })),
    [billing.charge_presets, billing.charge_presets_display]
  )

  const initialAmount = presets[0]?.amount ?? billing.min_usd ?? ''
  const [amount, setAmount] = useState(initialAmount)
  const flow = useChargeFlow()
  const busy = flow.phase === 'charging' || flow.phase === 'polling'
  const clampedAmount = clampAmount(amount, billing)
  const canBuy = !busy && clampedAmount !== ''

  const startBuy = () => {
    if (!clampedAmount) {
      return
    }

    setAmount(clampedAmount)
    void flow.start(clampedAmount)
  }

  return (
    <ListRow
      action={
        <div className="flex min-w-0 flex-wrap items-center justify-start gap-2 @2xl:justify-end">
          {presets.map(preset => (
            <Button
              aria-pressed={amount === preset.amount}
              disabled={busy}
              key={preset.amount}
              onClick={() => setAmount(preset.amount)}
              size="sm"
              type="button"
              variant={amount === preset.amount ? 'default' : 'outline'}
            >
              {preset.label}
            </Button>
          ))}
          <Input
            aria-label="Custom credit amount"
            className="h-8 w-24"
            disabled={busy}
            inputMode="decimal"
            max={billing.max_usd ?? undefined}
            min={billing.min_usd ?? undefined}
            onBlur={() => setAmount(clampedAmount)}
            onChange={event => {
              flow.reset()
              setAmount(event.target.value)
            }}
            placeholder={billing.min_usd ? formatMoney(billing.min_usd) : '$'}
            step="0.01"
            type="number"
            value={amount}
          />
          <Button disabled={!canBuy} onClick={startBuy} size="sm" type="button" variant="outline">
            Buy
          </Button>
        </div>
      }
      below={
        <BuyCreditsOutcome
          amount={clampedAmount}
          busy={busy}
          onPortal={openExternal}
          onRetry={() => {
            if (!clampedAmount) {
              return
            }

            void flow.start(clampedAmount)
          }}
          outcome={flow.outcome}
        />
      }
      description={row.description}
      key={row.id}
      title={row.title}
    />
  )
}

function BuyCreditsOutcome({
  amount,
  busy,
  onPortal,
  onRetry,
  outcome
}: {
  amount: string
  busy: boolean
  onPortal: (url?: string) => void
  onRetry: () => void
  outcome: ReturnType<typeof useChargeFlow>['outcome']
}) {
  if (busy) {
    return (
      <div className="mt-2 text-[length:var(--conversation-caption-font-size)] text-(--ui-text-tertiary)">
        Processing… checking settlement
      </div>
    )
  }

  if (!outcome) {
    return null
  }

  if (outcome.kind === 'success') {
    return (
      <div className="mt-2 text-[length:var(--conversation-caption-font-size)] text-(--ui-text-tertiary)">
        {formatMoney(outcome.amountUsd ?? amount)} added. Balance is refreshing.
      </div>
    )
  }

  if (outcome.kind === 'ambiguous') {
    return (
      <div className="mt-2 flex min-w-0 flex-wrap items-center gap-2 text-[length:var(--conversation-caption-font-size)] text-(--ui-text-tertiary)">
        <span>
          {outcome.title}: {outcome.message}
        </span>
        {outcome.portalUrl && (
          <Button onClick={() => onPortal(outcome.portalUrl)} size="sm" type="button" variant="outline">
            Open portal
            <ExternalLink className="size-3.5" />
          </Button>
        )}
      </div>
    )
  }

  const portalUrl = outcome.action?.type === 'portal' ? outcome.action.url : undefined

  return (
    <div className="mt-2 flex min-w-0 flex-wrap items-center gap-2 text-[length:var(--conversation-caption-font-size)] text-(--ui-text-tertiary)">
      <span>
        {outcome.title}: {outcome.message}
      </span>
      {outcome.action?.type === 'retry' && (
        <Button onClick={onRetry} size="sm" type="button" variant="outline">
          Retry
        </Button>
      )}
      {portalUrl && (
        <Button onClick={() => onPortal(portalUrl)} size="sm" type="button" variant="outline">
          Open portal
          <ExternalLink className="size-3.5" />
        </Button>
      )}
    </div>
  )
}

function UsageBar({ bar }: { bar: NonNullable<BillingUsageRowView['bar']> }) {
  return (
    <div
      aria-label={bar.label}
      aria-valuemax={100}
      aria-valuemin={0}
      aria-valuenow={Math.round(bar.value * 100)}
      className="h-[5px] w-full overflow-hidden rounded-full bg-muted"
      role="progressbar"
    >
      <div
        className={cn('h-full rounded-full', bar.tone === 'subscription' ? 'bg-primary' : 'bg-muted-foreground/45')}
        style={{ width: `${Math.round(bar.value * 100)}%` }}
      />
    </div>
  )
}

function UsageRow({ row }: { row: BillingUsageRowView }) {
  return (
    <div className="py-3">
      <div className="flex min-w-0 items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="text-[length:var(--conversation-text-font-size)] font-medium text-foreground">
            {row.title}
          </div>
          <div className="mt-1 text-[length:var(--conversation-caption-font-size)] text-(--ui-text-tertiary)">
            {row.caption}
          </div>
        </div>
        <div className="shrink-0 text-right text-[length:var(--conversation-text-font-size)] font-medium text-foreground">
          {row.value}
        </div>
      </div>
      {row.bar && (
        <div className="mt-2">
          <UsageBar bar={row.bar} />
        </div>
      )}
    </div>
  )
}

export function BillingSettings() {
  const billingState = useBillingState()
  const subscriptionState = useSubscriptionState()
  const view = deriveBillingView(billingState.data, subscriptionState.data)
  const billing = billingState.data?.ok ? billingState.data.data : undefined

  return (
    <SettingsContent>
      <SectionHeading icon={BarChart3} title="Billing" />

      <div className="@container mb-5">
        <div className="grid gap-3 rounded-lg border border-border/70 bg-muted/20 p-4 @2xl:grid-cols-3">
          {view.summary.map(item => (
            <SummaryCard key={item.label} label={item.label} tone={item.tone} value={item.value} />
          ))}
        </div>
      </div>

      {view.notice && <NoticeCard notice={view.notice} />}

      {view.accountRows.length > 0 && (
        <>
          <SectionHeading icon={BarChart3} title="Account" />
          {view.accountRows.map(row => (
            <AccountRow billing={billing} key={row.id} row={row} />
          ))}
        </>
      )}

      {view.usageRows.length > 0 && (
        <>
          <SectionHeading icon={BarChart3} title="Usage" />
          {view.usageRows.map(row => (
            <UsageRow key={row.id} row={row} />
          ))}
        </>
      )}

      {
        // no endpoint yet — NAS capability-board gap
        FEATURE_BILLING_INVOICES ? <SectionHeading icon={BarChart3} title="Invoices" /> : null
      }
    </SettingsContent>
  )
}

function clampAmount(raw: string, billing: Pick<BillingStateResponse, 'max_usd' | 'min_usd'>): string {
  const amount = parseAmount(raw)

  if (amount == null) {
    return ''
  }

  const min = parseAmount(billing.min_usd)
  const max = parseAmount(billing.max_usd)
  const clampedMin = min == null ? amount : Math.max(min, amount)
  const clamped = max == null ? clampedMin : Math.min(max, clampedMin)

  return formatAmountForRequest(clamped)
}

function parseAmount(value?: null | number | string): null | number {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null
  }

  if (typeof value !== 'string') {
    return null
  }

  const parsed = Number(value.replace(/[$,\s]/g, ''))

  return Number.isFinite(parsed) && parsed > 0 ? parsed : null
}

function formatAmountForRequest(value: number): string {
  return Number.isInteger(value) ? String(value) : value.toFixed(2).replace(/0+$/, '').replace(/\.$/, '')
}

function formatMoney(value?: null | number | string): string {
  const amount = parseAmount(value)

  if (amount == null) {
    return EMPTY_BILLING_VALUE
  }

  return new Intl.NumberFormat(undefined, {
    currency: 'USD',
    maximumFractionDigits: amount % 1 === 0 ? 0 : 2,
    minimumFractionDigits: amount % 1 === 0 ? 0 : 2,
    style: 'currency'
  }).format(amount)
}
