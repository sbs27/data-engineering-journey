#!/bin/bash

echo "ETL Pipeline Deployment Verification"
echo "==================================="
echo ""

echo "1. Original ETL Service:"
echo "   URL: https://my-etl-pipeline-d5wd25vdka-uc.a.run.app"
echo "   Test: curl https://my-etl-pipeline-d5wd25vdka-uc.a.run.app/health"
echo ""

echo "2. Scheduled ETL Service:"
echo "   URL: https://etl-scheduled-d5wd25vdka-uc.a.run.app"
echo "   Test: curl https://etl-scheduled-d5wd25vdka-uc.a.run.app/health"
echo ""

echo "3. Trigger Scheduled ETL:"
echo "   curl https://etl-scheduled-d5wd25vdka-uc.a.run.app/run-now"
echo ""

echo "4. View Logs:"
echo "   gcloud run logs tail etl-scheduled --region us-central1"
echo ""

echo "5. List Services:"
echo "   gcloud run services list --region us-central1"
