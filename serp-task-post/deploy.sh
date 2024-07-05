#!/bin/bash

# Step 1: Compile TypeScript
npx tsc

# Step 2: Create deployment directory
mkdir -p deploy
cp -r dist/* deploy/
cp -r node_modules deploy/

# Step 3: Zip the deployment package
cd deploy
zip -r lambda_function.zip *

# Step 4: Deploy to AWS Lambda
aws lambda update-function-code --function-name serp-post --zip-file fileb://lambda_function.zip

# Step 5: Clean up
cd ..
rm -rf deploy