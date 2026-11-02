# reset_mysql_password.ps1
# Run this script as Administrator to reset MySQL root password to 'root'
# -------------------------------------------------------------------------
# Usage: Right-click PowerShell -> Run as Administrator
#        Then run: .\reset_mysql_password.ps1

Write-Host "=== MySQL Root Password Reset Script ===" -ForegroundColor Cyan
Write-Host "This script will reset your MySQL root password to 'root'" -ForegroundColor Yellow
Write-Host ""

$mysqlBin = "C:\Program Files\MySQL\MySQL Server 8.0\bin"
$mysqlExe  = "$mysqlBin\mysql.exe"
$mysqldExe = "$mysqlBin\mysqld.exe"

# Step 1: Stop MySQL service
Write-Host "[1] Stopping MySQL80 service..." -ForegroundColor Green
Stop-Service MySQL80 -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 3

# Step 2: Start mysqld with skip-grant-tables in background
Write-Host "[2] Starting MySQL with --skip-grant-tables..." -ForegroundColor Green
$mysqldProc = Start-Process -FilePath $mysqldExe `
    -ArgumentList "--skip-grant-tables --skip-networking" `
    -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 5

# Step 3: Run SQL to reset password
Write-Host "[3] Resetting root password to 'root'..." -ForegroundColor Green
$sql = "FLUSH PRIVILEGES; ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'root'; FLUSH PRIVILEGES;"
& $mysqlExe -u root --connect-expired-password -e $sql 2>&1

# Step 4: Kill the skip-grant mysqld and restart service
Write-Host "[4] Restarting MySQL service normally..." -ForegroundColor Green
Stop-Process -Id $mysqldProc.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2
Start-Service MySQL80
Start-Sleep -Seconds 3

# Step 5: Verify
Write-Host "[5] Verifying connection with new password..." -ForegroundColor Green
$result = & $mysqlExe -u root --password="root" -e "SELECT 'Connection successful!' AS Status;" 2>&1
Write-Host $result

Write-Host ""
Write-Host "=== Done! MySQL root password is now: root ===" -ForegroundColor Cyan
Write-Host "You can now run the pipeline: python elt_batch_pipeline.py" -ForegroundColor Green
