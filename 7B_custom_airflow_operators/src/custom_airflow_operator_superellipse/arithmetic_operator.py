from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ArithmeticOperator(BaseOperator):
    @apply_defaults
    def __init__(self, num1, num2, operation, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.num1 = num1
        self.num2 = num2
        self.operation = operation

    def execute(self, context):
        if self.operation == 'add':
            result = self.num1 + self.num2
        elif self.operation == 'subtract':
            result = self.num1 - self.num2
        elif self.operation == 'multiply':
            result = self.num1 * self.num2
        elif self.operation == 'divide':
            if self.num2 == 0:
                raise ValueError("Division by zero is not allowed")
            result = self.num1 / self.num2
        else:
            raise ValueError(f"Unsupported operation: {self.operation}")
        
        self.log.info(f"The result of {self.operation} operation is: {result}")
        return result


class MyClass:
  x = 5