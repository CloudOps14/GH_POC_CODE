$timestamp = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000000

$body = @{
    streams = @(
        @{
            stream = @{
                job = "test"
            }
            values = @(
                @(
                    "$timestamp",
                    "Hello Loki"
                )
            )
        }
    )
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
  -Uri "http://localhost:3100/loki/api/v1/push" `
  -Method Post `
  -ContentType "application/json" `
  -Body $body