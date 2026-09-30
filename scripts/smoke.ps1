# Requires PowerShell 7 and the running Compose stack. Mutates demo data only.
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
function Sql($service, $database, $query) {
    $result = docker compose exec -T $service psql -U demo -d $database -tAc $query
    if ($LASTEXITCODE -ne 0) { throw "SQL failed: $query" }
    return "$result".Trim()
}
function Wait-Until($check, $label) {
    for ($i = 0; $i -lt 90; $i++) {
        if (& $check) { return }
        Start-Sleep -Seconds 1
    }
    throw "Timed out: $label"
}
Wait-Until { try { (Invoke-WebRequest http://localhost:5080/health).StatusCode -eq 200 } catch { $false } } 'orders startup'
Wait-Until { try { (Invoke-WebRequest http://localhost:5081/health).StatusCode -eq 200 } catch { $false } } 'billing startup'
$order = Invoke-RestMethod http://localhost:5080/orders -Method Post -ContentType 'application/json' -Body '{"amount":125.50,"currency":"EUR"}'
$id = $order.orderId
$eventId = $order.eventId
Wait-Until { (Sql billing-db billing "SELECT count(*) FROM invoices WHERE order_id='$id'") -eq '1' } 'invoice'
$invoice = Invoke-RestMethod "http://localhost:5081/invoices/$id"
if ($invoice.amount -ne 125.50 -or $invoice.currency -ne 'EUR') { throw 'Invoice data mismatch' }
Wait-Until { (Sql orders-db orders "SELECT count(*) FROM outbox WHERE id='$eventId' AND published_at IS NOT NULL") -eq '1' } 'published flag'
# Replay the same event through the real publisher and Kafka.
Sql orders-db orders "UPDATE outbox SET published_at=NULL WHERE id='$eventId'" | Out-Null
Wait-Until { (Sql orders-db orders "SELECT count(*) FROM outbox WHERE id='$eventId' AND published_at IS NOT NULL") -eq '1' } 'republish'
Wait-Until { (docker compose logs billing-api 2>&1 | Out-String) -match "$eventId`: duplicate skipped" } 'duplicate processed'
if ((Sql billing-db billing "SELECT count(*) FROM invoices WHERE order_id='$id'") -ne '1') { throw 'Duplicate invoice' }
if ((Sql billing-db billing "SELECT count(*) FROM inbox WHERE event_id='$eventId'") -ne '1') { throw 'Duplicate inbox row' }
# Outage: order is accepted with Kafka unavailable and later delivered.
docker compose stop kafka
if ($LASTEXITCODE -ne 0) { throw 'Cannot stop Kafka' }
try {
    $pending = Invoke-RestMethod http://localhost:5080/orders -Method Post -ContentType 'application/json' -Body '{"amount":42,"currency":"USD"}'
    if ((Sql orders-db orders "SELECT count(*) FROM outbox WHERE id='$($pending.eventId)' AND published_at IS NULL") -ne '1') { throw 'Missing pending event' }
} finally {
    docker compose start kafka
    if ($LASTEXITCODE -ne 0) { throw 'Cannot restart Kafka' }
}
Wait-Until { (Sql billing-db billing "SELECT count(*) FROM invoices WHERE order_id='$($pending.orderId)'") -eq '1' } 'outage recovery'
Write-Host 'PASS: delivery, payload, duplicate suppression, Kafka outage recovery'
