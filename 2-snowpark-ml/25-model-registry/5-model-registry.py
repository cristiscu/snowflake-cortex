# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry

from snowflake.ml.registry import Registry

reg = Registry(session=sp_session, database_name="ML", schema_name="REGISTRY")

mv = reg.log_model(clf,
                   model_name="my_model",
                   version_name="1",
                   conda_dependencies=["scikit-learn"],
                   comment="My awesome ML model",
                   metrics={"score": 96},
                   sample_input_data=train_features)

reg.delete_model("mymodel")
model_df = reg.show_models()
model_list = reg.models()
m = reg.get_model("MyModel")

print(m.comment)
m.comment = "A better description than the one I provided originally"
print(m.description)
m.description = "A better description than the one I provided originally"

print(m.show_tags())
m.set_tag("live_version", "1")
m.get_tag("live_version")
m.unset_tag("live_version")

version_list = m.versions()
version_df = m.show_versions()
m.delete_version("rc1")

default_version = m.default
m.default = "2"

mv = m.version("1")
mv = m.default

print(mv.comment)
print(mv.description)

mv.comment = "A model version comment"
mv.description = "Same as setting the comment"

# metrics
from sklearn import metrics

test_accuracy = metrics.accuracy_score(test_labels, prediction)
test_confusion_matrix = metrics.confusion_matrix(test_labels, prediction)

# scalar metric
mv.set_metric("test_accuracy", test_accuracy)

# hierarchical (dictionary) metric
mv.set_metric("evaluation_info", {"dataset_used": "my_dataset", "accuracy": test_accuracy, "f1_score": f1_score})

# multivalent (matrix) metric
mv.set_metric("confusion_matrix", test_confusion_matrix)

metrics = mv.show_metrics()
mv.delete_metric("test_accuracy")

remote_prediction = mv.run(test_features, function_name="predict")