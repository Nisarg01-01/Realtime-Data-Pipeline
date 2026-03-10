param(
    [switch]$SkipDocker = $false,
    [switch]$SkipHealthCheck = $false
)

$ErrorActionPreference = "Stop"

Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   Real-Time Data Pipeline - Complete System Setup         ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

# ============================================================================
# STEP 1: Check Prerequisites
# ============================================================================
Write-Host "`n[1/6] Checking prerequisites..." -ForegroundColor Yellow

$missingTools = @()
if (-not (Get-Command conda -ErrorAction SilentlyContinue)) {
    $missingTools += "Conda"
}
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    $missingTools += "Docker"
}
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    $missingTools += "Git"
}

if ($missingTools.Count -gt 0) {
    Write-Host "✗ Missing prerequisites: $($missingTools -join ', ')" -ForegroundColor Red
    Write-Host "  Please install the missing tools and try again." -ForegroundColor Yellow
    exit 1
}

Write-Host "✓ All prerequisites found (Conda, Docker, Git)" -ForegroundColor Green

# ============================================================================
# STEP 2: Create or Activate Conda Environment
# ============================================================================
Write-Host "`n[2/6] Setting up Conda environment..." -ForegroundColor Yellow

$envName = "realtime-pipeline"
$pythonVersion = "3.11"

# Check if environment exists
$envExists = conda env list | Select-String $envName
if ($envExists) {
    Write-Host "✓ Environment '$envName' already exists" -ForegroundColor Green
} else {
    Write-Host "  Creating new environment: $envName (Python $pythonVersion)..." -ForegroundColor Cyan
    conda create -n $envName python=$pythonVersion -y
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ Failed to create conda environment" -ForegroundColor Red
        exit 1
    }
    Write-Host "✓ Environment created successfully" -ForegroundColor Green
}

# Activate environment
Write-Host "  Activating environment..." -ForegroundColor Cyan
conda activate $envName
if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Failed to activate conda environment" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Environment activated: $envName" -ForegroundColor Green

# ============================================================================
# STEP 3: Install Python Dependencies
# ============================================================================
Write-Host "`n[3/6] Installing Python dependencies from requirements.txt..." -ForegroundColor Yellow

if (-not (Test-Path "requirements.txt")) {
    Write-Host "✗ requirements.txt not found in current directory" -ForegroundColor Red
    exit 1
}

pip install -r requirements.txt --quiet
if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Failed to install dependencies" -ForegroundColor Red
    exit 1
}
Write-Host "✓ All dependencies installed successfully" -ForegroundColor Green

# ============================================================================
# STEP 4: Configure PySpark Environment Variables
# ============================================================================
Write-Host "`n[4/6] Configuring PySpark environment..." -ForegroundColor Yellow

try {
    $pythonPath = python -c "import sys; print(sys.executable)" 2>$null
    if (-not $pythonPath) {
        throw "Could not determine Python path"
    }
} catch {
    Write-Host "✗ Could not determine Python executable path" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $pythonPath)) {
    Write-Host "✗ Python executable not found at: $pythonPath" -ForegroundColor Red
    exit 1
}

$env:PYSPARK_PYTHON = $pythonPath
$env:PYSPARK_DRIVER_PYTHON = $pythonPath
$env:PYSPARK_PIN_THREAD = "true"

Write-Host "✓ PySpark environment configured:" -ForegroundColor Green
Write-Host "  PYSPARK_PYTHON = $pythonPath" -ForegroundColor Cyan
Write-Host "  PYSPARK_DRIVER_PYTHON = $pythonPath" -ForegroundColor Cyan
Write-Host "  PYSPARK_PIN_THREAD = true" -ForegroundColor Cyan

$pythonVersion = python --version 2>&1
Write-Host "  Python version: $pythonVersion" -ForegroundColor Cyan

# ============================================================================
# STEP 5: Start Docker Services
# ============================================================================
if (-not $SkipDocker) {
    Write-Host "`n[5/6] Starting Docker services (Kafka, PostgreSQL, Zookeeper)..." -ForegroundColor Yellow
    
    if (-not (Test-Path "docker-compose.yml")) {
        Write-Host "✗ docker-compose.yml not found" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "  Starting containers..." -ForegroundColor Cyan
    docker compose down -q 2>$null  # Clean up any existing containers
    docker compose up -d
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ Failed to start Docker services" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "  Waiting for services to be ready..." -ForegroundColor Cyan
    Start-Sleep -Seconds 5
    
    $services = docker compose ps --services
    Write-Host "✓ Docker services started:" -ForegroundColor Green
    foreach ($service in $services) {
        Write-Host "  • $service" -ForegroundColor Cyan
    }
} else {
    Write-Host "`n[5/6] Skipping Docker services (--SkipDocker flag used)" -ForegroundColor Yellow
}

# ============================================================================
# STEP 6: Run Health Check
# ============================================================================
if (-not $SkipHealthCheck) {
    Write-Host "`n[6/6] Running system health check..." -ForegroundColor Yellow
    
    if (-not (Test-Path "health_check.py")) {
        Write-Host "⚠ health_check.py not found - skipping health check" -ForegroundColor Yellow
    } else {
        python health_check.py
        if ($LASTEXITCODE -ne 0) {
            Write-Host "⚠ Some health checks failed - review output above" -ForegroundColor Yellow
        } else {
            Write-Host "✓ Health check passed" -ForegroundColor Green
        }
    }
} else {
    Write-Host "`n[6/6] Skipping health check (--SkipHealthCheck flag used)" -ForegroundColor Yellow
}

# ============================================================================
# SETUP COMPLETE
# ============================================================================
Write-Host "`n╔════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║   ✓ System Setup Complete!                                 ║" -ForegroundColor Green
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Green

Write-Host "`nYou can now run:" -ForegroundColor Cyan
Write-Host "  pytest tests/ -v                            # All tests" -ForegroundColor White
Write-Host "  pytest tests/test_performance.py -v -s      # Performance tests" -ForegroundColor White
Write-Host "  python producer.py                          # Start producer" -ForegroundColor White
Write-Host "  python consumer_spark.py                    # Start consumer" -ForegroundColor White
Write-Host "  python build_aggregates.py                  # Build analytics" -ForegroundColor White
Write-Host "  streamlit run dashboard.py                  # Launch dashboard" -ForegroundColor White

Write-Host "`nNote: If you open a new terminal, you'll need to activate the environment:" -ForegroundColor Yellow
Write-Host "  conda activate $envName" -ForegroundColor Cyan
