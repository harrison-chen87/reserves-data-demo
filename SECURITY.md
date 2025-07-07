# 🔒 Security Guide

## Overview

This ValNav Synthetic Data Generator handles sensitive Databricks credentials and requires proper security practices to protect your access tokens and workspace information.

## 🛡️ Built-in Protection

### `.gitignore` File

The project includes a comprehensive `.gitignore` file that automatically protects:

```
# Environment Variables and Credentials
.env
.env.local
.env.development
.env.production
.env.staging
.env.test
*.env

# Databricks Configuration Files
databricks.cfg
.databrickscfg

# Virtual Environments
valnav-dash-env/
env/
venv/
```

### What Gets Protected

✅ **Protected from version control**:
- `.env` files containing Databricks credentials
- Virtual environment directories
- Python cache files (`__pycache__/`, `*.pyc`)
- Temporary files and logs
- IDE/editor configuration files
- OS-specific files (`.DS_Store`, `Thumbs.db`)

✅ **Safe to commit**:
- Source code files (`*.py`)
- Documentation files (`*.md`)
- Configuration templates
- Requirements and setup scripts

## 🔑 Credential Management

### Secure Methods (Recommended)

1. **`.env` File** (Best for development):
   ```bash
   # Create .env file
   ./create_env.sh
   ```

2. **Environment Variables** (Best for production):
   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-personal-access-token"
   ```

3. **Interactive Setup**:
   ```bash
   ./setup_environment.sh
   ```

### Insecure Methods (NEVER DO)

❌ **Never hardcode credentials in source code**:
```python
# DON'T DO THIS!
DATABRICKS_HOST = "https://my-workspace.cloud.databricks.com"
DATABRICKS_TOKEN = "dapi1234567890abcdef..."
```

❌ **Never commit credentials to git**:
```bash
# DON'T DO THIS!
git add .env
git commit -m "Added my credentials"
```

## ⚠️ Security Incident Response

### If You Accidentally Commit Credentials

**Immediate Actions**:

1. **🚨 STOP** - Don't push to remote repository if you haven't already
2. **🔒 REVOKE** the exposed token immediately:
   - Go to Databricks workspace → User Settings → Developer → Access tokens
   - Find the compromised token and revoke it
3. **🔄 GENERATE** a new personal access token
4. **🔧 UPDATE** your local `.env` file with the new token

**If Already Pushed to Remote**:

1. **🚨 REVOKE** the token immediately (step 2 above)
2. **📞 NOTIFY** your team/security team about the exposure
3. **🧹 CLEAN** git history (advanced):
   ```bash
   # Remove the file from git history (use with caution)
   git filter-branch --force --index-filter \
   'git rm --cached --ignore-unmatch .env' \
   --prune-empty --tag-name-filter cat -- --all
   ```
4. **🔄 FORCE** push the cleaned history (if appropriate)

### Prevention Checklist

- [ ] `.gitignore` file is in place
- [ ] `.env` files are listed in `.gitignore`
- [ ] Test that `.env` files don't appear in `git status`
- [ ] Use secure credential storage methods only
- [ ] Regularly rotate your access tokens
- [ ] Never share credentials via email, chat, or screenshots

## 🔍 Security Verification

### Check What Git Tracks

```bash
# Verify .env files are ignored
echo "DATABRICKS_HOST=test" > .env
git status  # Should NOT show .env file
rm .env
```

### Verify `.gitignore` is Working

```bash
# Check git status - should not show:
# - valnav-dash-env/
# - __pycache__/
# - *.pyc files
# - .env files
git status
```

### Test Configuration Protection

```bash
# Create test credentials
./create_env.sh  # Enter test values

# Verify git doesn't see them
git status  # Should not show .env

# Clean up
rm .env
```

## 🚀 Production Deployment

### Environment Variables (Recommended)

For production deployments, use environment variables instead of `.env` files:

```bash
# In your deployment environment
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-production-token"

# Run the application
python app.py
```

### Container/Docker Deployment

```dockerfile
# In your Dockerfile
ENV DATABRICKS_HOST=""
ENV DATABRICKS_TOKEN=""

# Pass values at runtime
docker run -e DATABRICKS_HOST="..." -e DATABRICKS_TOKEN="..." your-app
```

### Cloud Platform Secrets

Use your cloud platform's secret management:
- **AWS**: AWS Secrets Manager, Parameter Store
- **Azure**: Azure Key Vault
- **GCP**: Secret Manager
- **Kubernetes**: Secrets

## 📞 Support

If you suspect a security incident or need help with credential management:

1. **Immediate security concerns**: Revoke compromised tokens immediately
2. **Setup help**: Use the troubleshooting guides in `TROUBLESHOOTING.md`
3. **Best practices**: Follow this security guide

Remember: **When in doubt, revoke and regenerate tokens** - it's always safer than leaving potentially compromised credentials active. 