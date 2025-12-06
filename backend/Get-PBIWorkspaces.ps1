param(
    [Parameter(Mandatory=$false)]
    [string]$TenantId = "common",
    [Parameter(Mandatory=$false)]
    [switch]$Raw,
    [Parameter(Mandatory=$false)]
    [ValidateSet("workspaces", "models", "refreshes", "trigger", "reports", "schedule", "takeover")]
    [string]$Mode = "workspaces",
    [Parameter(Mandatory=$false)]
    [string]$WorkspaceId,
    [Parameter(Mandatory=$false)]
    [string]$DatasetId,
    [Parameter(Mandatory=$false)]
    [int]$Top = 10,
    [Parameter(Mandatory=$false)]
    [int]$TokenCacheHours = 2,
    [Parameter(Mandatory=$false)]
    [string]$ScheduleJson
)

$TokenCachePath = Join-Path $PSScriptRoot "pbi_token_cache.json"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

function Get-CachedAuthHeader {
    if (-not (Test-Path $TokenCachePath)) { return $null }
    try {
        $cache = Get-Content $TokenCachePath -Raw | ConvertFrom-Json
        if (-not $cache.Authorization -or -not $cache.ExpiresAt) { return $null }
        $expires = [datetime]$cache.ExpiresAt
        if ($expires -gt (Get-Date)) {
            return $cache.Authorization
        }
    }
    catch {
        return $null
    }
    return $null
}

function Save-AuthHeader {
    param(
        [string]$AuthHeader,
        [datetime]$ExpiresAt
    )
    if (-not $ExpiresAt) {
        $ExpiresAt = (Get-Date).AddHours($TokenCacheHours)
    }
    $payload = [pscustomobject]@{
        Authorization = $AuthHeader
        ExpiresAt     = $ExpiresAt.ToString("o")
        CachedAt      = (Get-Date).ToString("o")
    }
    $payload | ConvertTo-Json | Set-Content -Path $TokenCachePath -Encoding ASCII
}

function Acquire-AuthHeader {
    $cached = Get-CachedAuthHeader
    if ($cached) { return $cached }

    Import-Module MicrosoftPowerBIMgmt.Profile -ErrorAction Stop
    Connect-PowerBIServiceAccount | Out-Null

    $tokenResponse = Get-PowerBIAccessToken
    $expiresAt = $null

    if ($tokenResponse.PSObject.Properties.Name -contains "Values") {
        $authHeader = $tokenResponse.Authorization
        if ($tokenResponse.PSObject.Properties.Name -contains "ExpiresOn") {
            $expiresAt = [datetime]$tokenResponse.ExpiresOn
        }
    }
    elseif ($tokenResponse -is [string]) {
        if ($tokenResponse.TrimStart() -like "{*") {
            $tokenJson = $tokenResponse | ConvertFrom-Json
            $authHeader = $tokenJson.access_token
            if ($tokenJson.expires_on) {
                $expiresAt = [datetime]::Parse($tokenJson.expires_on)
            }
            elseif ($tokenJson.expires_in) {
                $expiresAt = (Get-Date).AddSeconds([int]$tokenJson.expires_in)
            }
        }
        else {
            $authHeader = $tokenResponse
        }
    }
    else {
        $authHeader = $tokenResponse.accessToken
        if ($tokenResponse.PSObject.Properties.Name -contains "ExpiresOn") {
            $expiresAt = [datetime]$tokenResponse.ExpiresOn
        }
        elseif ($tokenResponse.PSObject.Properties.Name -contains "ExpiresIn") {
            $expiresAt = (Get-Date).AddSeconds([int]$tokenResponse.ExpiresIn)
        }
    }

    if (-not $authHeader) {
        throw "Unable to obtain access token/authorization header"
    }

    $maxCache = (Get-Date).AddHours($TokenCacheHours)
    if ($expiresAt) {
        if ($TokenCacheHours -gt 0 -and $expiresAt -gt $maxCache) {
            $expiresAt = $maxCache
        }
    }
    else {
        $expiresAt = $maxCache
    }

    Save-AuthHeader -AuthHeader $authHeader -ExpiresAt $expiresAt
    return $authHeader
}

function Get-PBIWorkspaces {
    param(
        [string]$AuthHeader
    )
    $headers = @{ Authorization = $AuthHeader }
    Invoke-RestMethod -Method Get -Headers $headers -Uri "https://api.powerbi.com/v1.0/myorg/groups"
}

function Get-PBISemanticModels {
    param(
        [string]$AuthHeader,
        [string]$WorkspaceId
    )
    if (-not $WorkspaceId) { throw "WorkspaceId is required when Mode=models" }
    $headers = @{ Authorization = $AuthHeader }
    Invoke-RestMethod -Method Get -Headers $headers -Uri "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/datasets"
}

function Get-PBIRefreshes {
    param(
        [string]$AuthHeader,
        [string]$WorkspaceId,
        [string]$DatasetId,
        [int]$Top
    )
    if (-not $WorkspaceId -or -not $DatasetId) { throw "WorkspaceId and DatasetId are required when Mode=refreshes" }
    $headers = @{ Authorization = $AuthHeader }
    $uri = "https://api.powerbi.com/v1.0/myorg/datasets/$DatasetId/refreshes?`$top=$Top"
    Invoke-RestMethod -Method Get -Headers $headers -Uri $uri
}

function Get-PBIWorkspaceReports {
    param(
        [string]$AuthHeader,
        [string]$WorkspaceId
    )
    if (-not $WorkspaceId) { throw "WorkspaceId is required when Mode=reports" }
    $headers = @{ Authorization = $AuthHeader }
    $uri = "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/reports"
    Invoke-RestMethod -Method Get -Headers $headers -Uri $uri
}

function Trigger-PBIRefresh {
    param(
        [string]$AuthHeader,
        [string]$WorkspaceId,
        [string]$DatasetId
    )
    if (-not $WorkspaceId -or -not $DatasetId) { throw "WorkspaceId and DatasetId are required when triggering a refresh" }
    $headers = @{ Authorization = $AuthHeader; "Content-Type" = "application/json" }
    $uri = "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/datasets/$DatasetId/refreshes"
    $body = @{ type = "dataOnly" } | ConvertTo-Json
    Invoke-RestMethod -Method Post -Headers $headers -Uri $uri -Body $body
}

function Get-PBIRefreshSchedule {
    param(
        [string]$AuthHeader,
        [string]$WorkspaceId,
        [string]$DatasetId
    )
    if (-not $WorkspaceId -or -not $DatasetId) { throw "WorkspaceId and DatasetId are required when reading refresh schedule" }
    $headers = @{ Authorization = $AuthHeader }
    $uri = "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/datasets/$DatasetId/refreshSchedule"
    Invoke-RestMethod -Method Get -Headers $headers -Uri $uri
}

function Set-PBIRefreshSchedule {
    param(
        [string]$AuthHeader,
        [string]$WorkspaceId,
        [string]$DatasetId,
        [string]$ScheduleJson
    )
    if (-not $WorkspaceId -or -not $DatasetId) { throw "WorkspaceId and DatasetId are required when updating refresh schedule" }
    if (-not $ScheduleJson) { throw "ScheduleJson is required when updating refresh schedule" }
    $headers = @{ Authorization = $AuthHeader; "Content-Type" = "application/json" }
    $uri = "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/datasets/$DatasetId/refreshSchedule"
    Invoke-RestMethod -Method Patch -Headers $headers -Uri $uri -Body $ScheduleJson
}

function TakeOver-PBIDataset {
    param(
        [string]$AuthHeader,
        [string]$WorkspaceId,
        [string]$DatasetId
    )
    if (-not $WorkspaceId -or -not $DatasetId) { throw "WorkspaceId and DatasetId are required when taking over a dataset" }
    $headers = @{ Authorization = $AuthHeader }
    $uri = "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/datasets/$DatasetId/Default.TakeOver"
    Invoke-RestMethod -Method Post -Headers $headers -Uri $uri
}

try {
    $authHeader = Acquire-AuthHeader

    if ($Mode -eq "models") {
        $data = Get-PBISemanticModels -AuthHeader $authHeader -WorkspaceId $WorkspaceId
        $payload = @{
            workspaceId = $WorkspaceId
            datasets    = @()
        }
        foreach ($d in $data.value) {
            $payload.datasets += @{
                id                = $d.id
                name              = $d.name
                createdDate       = $d.createdDate
                configuredBy      = $d.configuredBy
                isRefreshable     = $d.isRefreshable
                targetStorageMode = $d.targetStorageMode
            }
        }
        if ($Raw) {
            $payload | ConvertTo-Json -Depth 12
            exit 0
        }
        $payload | ConvertTo-Json -Depth 6
        exit 0
    }

    if ($Mode -eq "refreshes") {
        $data = Get-PBIRefreshes -AuthHeader $authHeader -WorkspaceId $WorkspaceId -DatasetId $DatasetId -Top $Top
        $payload = @{
            workspaceId = $WorkspaceId
            datasetId   = $DatasetId
            refreshes   = $data.value
        }
        if ($Raw) {
            $payload | ConvertTo-Json -Depth 12
            exit 0
        }
        $payload | ConvertTo-Json -Depth 8
        exit 0
    }

    if ($Mode -eq "reports") {
        $data = Get-PBIWorkspaceReports -AuthHeader $authHeader -WorkspaceId $WorkspaceId
        $payload = @{
            workspaceId = $WorkspaceId
            reports     = $data.value
        }
        if ($Raw) {
            $payload | ConvertTo-Json -Depth 12
            exit 0
        }
        $payload | ConvertTo-Json -Depth 8
        exit 0
    }

    if ($Mode -eq "trigger") {
        $result = Trigger-PBIRefresh -AuthHeader $authHeader -WorkspaceId $WorkspaceId -DatasetId $DatasetId
        $payload = @{
            workspaceId = $WorkspaceId
            datasetId   = $DatasetId
            result      = $result
        }
        if ($Raw) {
            $payload | ConvertTo-Json -Depth 12
            exit 0
        }
        $payload | ConvertTo-Json -Depth 6
        exit 0
    }

    if ($Mode -eq "schedule") {
        if ($ScheduleJson) {
            $result = Set-PBIRefreshSchedule -AuthHeader $authHeader -WorkspaceId $WorkspaceId -DatasetId $DatasetId -ScheduleJson $ScheduleJson
            $payload = @{
                workspaceId = $WorkspaceId
                datasetId   = $DatasetId
                result      = $result
                updated     = $true
            }
        }
        else {
            $data = Get-PBIRefreshSchedule -AuthHeader $authHeader -WorkspaceId $WorkspaceId -DatasetId $DatasetId
            $payload = @{
                workspaceId = $WorkspaceId
                datasetId   = $DatasetId
                schedule    = $data
                updated     = $false
            }
        }
        if ($Raw) {
            $payload | ConvertTo-Json -Depth 12
            exit 0
        }
        $payload | ConvertTo-Json -Depth 8
        exit 0
    }

    if ($Mode -eq "takeover") {
        $result = TakeOver-PBIDataset -AuthHeader $authHeader -WorkspaceId $WorkspaceId -DatasetId $DatasetId
        $payload = @{
            workspaceId = $WorkspaceId
            datasetId   = $DatasetId
            result      = $result
        }
        if ($Raw) {
            $payload | ConvertTo-Json -Depth 12
            exit 0
        }
        $payload | ConvertTo-Json -Depth 6
        exit 0
    }

    $data = Get-PBIWorkspaces -AuthHeader $authHeader

    if ($Raw) {
        $data | ConvertTo-Json -Depth 12
        exit 0
    }

    $result = @{
        tenant      = $TenantId
        retrievedAt = (Get-Date).ToString("s")
        workspaces  = @()
    }

    foreach ($ws in $data.value) {
        $result.workspaces += @{
            id                      = $ws.id
            name                    = $ws.name
            type                    = $ws.type
            isOnDedicatedCapacity   = $ws.isOnDedicatedCapacity
            capacityId              = $ws.capacityId
            state                   = $ws.state
        }
    }

    $result | ConvertTo-Json -Depth 6
}
catch {
    $err = $_
    $errorRecord = @{
        message   = $err.Exception.Message
        category  = $err.CategoryInfo.Category
        details   = $err.ErrorDetails.Message
    }

    $errorRecord | ConvertTo-Json
    exit 1
}
