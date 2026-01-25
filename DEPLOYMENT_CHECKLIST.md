# Production Deployment Checklist

## Pre-Deployment

### 1. Environment Setup
- [ ] Set `FINLOOM_ENV=production`
- [ ] Configure S3 bucket: `FINLOOM_S3_BUCKET=your-bucket`
- [ ] Set Slack webhook: `SLACK_WEBHOOK_URL=https://...`
- [ ] Set PagerDuty key: `PAGERDUTY_INTEGRATION_KEY=...`
- [ ] Configure Jaeger (optional): `JAEGER_HOST=your-jaeger-host`
- [ ] Set up Redis (if using caching): `redis.host=your-redis-host`

### 2. Infrastructure
- [ ] Provision production database storage (SSD recommended)
- [ ] Set up S3 bucket with lifecycle policies
- [ ] Configure network security groups
- [ ] Set up load balancer (if applicable)
- [ ] Configure DNS records

### 3. Monitoring
- [ ] Deploy Prometheus server
- [ ] Deploy Grafana dashboards
- [ ] Set up Jaeger (optional)
- [ ] Configure alert rules
- [ ] Test Slack/PagerDuty integration

### 4. Database
- [ ] Initialize production database
- [ ] Run schema migrations
- [ ] Enable WAL mode (automatic)
- [ ] Configure automated backups
- [ ] Test disaster recovery procedure

## Deployment Steps

### 1. Build & Test
```bash
# Run all tests
pytest tests/ -v

# Run quality checks
python scripts/assess_quality.py --full

# Run performance tests
python scripts/perf_test.py

# Validate configuration
python finloom.py config validate
```

### 2. Deploy Application
```bash
# Docker deployment
docker build -t finloom:latest .
docker push your-registry/finloom:latest

# Kubernetes deployment
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/monitoring.yaml
```

### 3. Start Services
```bash
# Start monitoring
python finloom.py monitor start

# Verify health
curl http://localhost:8000/health/ready

# Check metrics
curl http://localhost:9090/metrics
```

### 4. Initial Data Load
```bash
# Backfill historical data
python scripts/01_backfill_historical.py

# Normalize financials
python scripts/normalize_all.py

# Verify data quality
python scripts/assess_quality.py --full
```

### 5. Create Initial Backup
```bash
# Create full backup
python scripts/backup_manager.py --full

# Verify backup in S3
python scripts/backup_manager.py --list
```

## Post-Deployment

### 1. Monitoring Verification
- [ ] Prometheus metrics flowing
- [ ] Grafana dashboards loading
- [ ] Health checks responding
- [ ] Alerts triggering correctly
- [ ] Logs being collected

### 2. Functional Testing
- [ ] Download new filing
- [ ] Parse XBRL data
- [ ] Normalize metrics
- [ ] Run quality checks
- [ ] Test cache (if enabled)
- [ ] Verify tracing (if enabled)

### 3. Performance Validation
```bash
# Run performance tests
python scripts/perf_test.py

# Expected results:
# - Query latency < 100ms
# - Throughput > 50 QPS
# - Cache hit ratio > 80% (if enabled)
# - Connection pool utilization < 80%
```

### 4. Disaster Recovery Test
```bash
# Create backup
python scripts/backup_manager.py --full

# Simulate failure (test environment only!)
mv data/finloom.duckdb data/finloom.duckdb.bak

# Restore from backup
python scripts/backup_manager.py --restore 20260125

# Verify data integrity
python scripts/assess_quality.py --reconcile
```

## Ongoing Operations

### Daily
- [ ] Check dashboard for anomalies
- [ ] Review error alerts
- [ ] Monitor disk space
- [ ] Check backup completion

### Weekly
- [ ] Run full quality assessment
- [ ] Review performance metrics
- [ ] Check for data drift
- [ ] Update financial data

### Monthly
- [ ] Test disaster recovery
- [ ] Review and tune alert thresholds
- [ ] Analyze query performance
- [ ] Update dependencies
- [ ] Review security logs

## Rollback Plan

### If Deployment Fails

1. **Stop new services**
   ```bash
   kubectl rollout undo deployment/finloom
   # or
   docker stop finloom-container
   ```

2. **Restore database**
   ```bash
   python scripts/backup_manager.py --restore YYYYMMDD
   ```

3. **Verify old version**
   ```bash
   python finloom.py status
   python scripts/assess_quality.py --reconcile
   ```

4. **Investigate issues**
   - Check logs: `tail -f logs/finloom.log`
   - Review metrics: Grafana dashboards
   - Check alerts: Slack/PagerDuty

## Performance Tuning

### Database Optimization
```bash
# Analyze partitioning needs
python finloom.py perf analyze

# Create partitions if recommended
python -c "from src.storage.partitioning import setup_partitioning; setup_partitioning(Database(), force=True)"

# Rebuild indexes
python -c "from src.storage.partitioning import TablePartitioner; TablePartitioner(Database()).optimize_indexes()"
```

### Cache Optimization
```bash
# Check cache hit ratio
python finloom.py cache stats

# Clear stale cache
python finloom.py cache clear

# Tune TTL in config/settings.production.yaml
```

### Connection Pool Tuning
Edit `config/settings.production.yaml`:
```yaml
database:
  pool_size: 10  # Increase for high concurrency
  timeout: 60    # Increase for long queries
```

## Security Hardening

### 1. Network Security
- [ ] Enable firewall rules
- [ ] Restrict database access
- [ ] Use VPC/private networking
- [ ] Enable SSL/TLS for Redis

### 2. Access Control
- [ ] Implement least-privilege IAM roles
- [ ] Rotate credentials regularly
- [ ] Enable audit logging
- [ ] Review access logs

### 3. Data Protection
- [ ] Enable S3 encryption
- [ ] Configure backup retention
- [ ] Implement data retention policies
- [ ] Test backup restoration

## Troubleshooting

### Common Issues

**Issue**: High memory usage
- **Solution**: Reduce connection pool size, enable query result limits

**Issue**: Slow queries
- **Solution**: Enable partitioning, add indexes, enable caching

**Issue**: Circuit breaker opening frequently
- **Solution**: Increase failure threshold, check SEC API status

**Issue**: Cache misses high
- **Solution**: Increase TTL, warm cache, check Redis health

**Issue**: Backup failures
- **Solution**: Check S3 permissions, verify disk space, review logs

## Contact & Support

- **Logs**: `/Users/V-Personal/FinLoom-2026/logs/`
- **Config**: `/Users/V-Personal/FinLoom-2026/config/`
- **Documentation**: `/Users/V-Personal/FinLoom-2026/docs/`
- **Issues**: GitHub Issues

---

**Last Updated**: January 25, 2026
**Version**: 1.0.0 Enterprise
