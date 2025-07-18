import functions_framework

@functions_framework.http
def hello_world(request):  # This is your Cloud Function
    return "Hello World! This is a test-run pushed from GitHub.", 200