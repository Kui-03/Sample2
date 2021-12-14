# HOW IT WORKS:

    1. Copy and paste contents of JSON-Test.txt as JSON POST object to "<URL>/predict".
    2. Optional params can be set:
        
        "model":
            "RF"    - Random Forest
            "XGB"   - XGBoost Binary Classifier
            "LR"    - Logistic Regression
            "SGD"   - SGD Classifier (test)
        
        "method":
            "predict"
            "predict_proba"
            "predict_log_proba"
            
        default parameters:
            "model":  "RF"
            "method": "predict"
            
    3. Features:
        ['city', 'job', 'category', 'merchant', 'age_category', 'dur', 'dist', 'amt', 'proxim_ave', 'rate_ave']
    
    4. Dataframe processed via pandas: df.to_json and pd.read_json()
        
        n = {"model": "RF", "method": "predict", df.to_json()}
        json.dump(n, <file.open("fn", "w")>)
        
    5. Frontend UI in progress..
