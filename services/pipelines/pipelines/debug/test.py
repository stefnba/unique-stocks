# %%


def job_logging(job_name: str):
    def decorator_func(original_func):
        def wrapper_func(*args, **kwargs):
            # Perform actions before the original function is called
            print(f"Decorator arguments: {job_name} {kwargs}")

            # Call the original function
            result = original_func(*args, **kwargs)

            # Perform actions after the original function is called
            print("Decorator finished")

            return result

        return wrapper_func

    return decorator_func


@job_logging(job_name="TETST JOBa")
def my_function(exhange_code: str):
    print("Original function")


my_function(exhange_code="XETRA")
