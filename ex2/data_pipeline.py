from abc import ABC, abstractmethod
from typing import Any
from typing import Protocol


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[str] = []
        self._rank: int = 0
        self._total_processed: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._data:
            raise Exception("No data available")

        rank = self._rank
        value = self._data.pop(0)
        self._rank += 1

        return rank, value


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            return all(isinstance(item, (int, float)) for item in data)

        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise Exception("Improper numeric data")

        if isinstance(data, list):
            for item in data:
                self._data.append(str(item))
                self._total_processed += 1
        else:
            self._data.append(str(data))
            self._total_processed += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True

        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)

        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise Exception("Improper text data")

        if isinstance(data, list):
            self._data.extend(data)
            self._total_processed += len(data)
        else:
            self._data.append(data)
            self._total_processed += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in data.items()
            )

        if isinstance(data, list):
            return all(self.validate(item) for item in data)

        return False

    def ingest(
        self,
        data: dict[str, str] | list[dict[str, str]],
    ) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")

        if isinstance(data, list):
            for item in data:
                self._data.append(
                    f"{item['log_level']}: "
                    f"{item['log_message']}"
                )
                self._total_processed += 1
        else:
            self._data.append(
                f"{data['log_level']}: "
                f"{data['log_message']}"
            )
            self._total_processed += 1


class ExportPlugin(Protocol):
    def process_output(
        self,
        data: list[tuple[int, str]]
    ) -> None:
        pass


class CSVExportPlugin:
    def process_output(
        self,
        data: list[tuple[int, str]]
    ) -> None:
        values = []

        for _, value in data:
            values.append(value)

        print("CSV Output:")
        print(",".join(values))


class JSONExportPlugin:
    def process_output(
        self,
        data: list[tuple[int, str]]
    ) -> None:
        items = []

        for rank, value in data:
            items.append(
                f'"item_{rank}": "{value}"'
            )

        print("JSON Output:")
        print("{" + ", ".join(items) + "}")


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, processor: DataProcessor) -> None:
        self._processors.append(processor)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            processed = False

            for processor in self._processors:
                if processor.validate(element):
                    processor.ingest(element)
                    processed = True
                    break

            if not processed:
                print(
                    "DataStream error - Can't process element in stream: "
                    f"{element}"
                )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self._processors:
            extracted_data: list[tuple[int, str]] = []

            for _ in range(nb):
                if not processor._data:
                    break

                extracted_data.append(processor.output())

            if extracted_data:
                plugin.process_output(extracted_data)

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")

        if not self._processors:
            print("No processor found, no data")
            return

        for processor in self._processors:
            name = processor.__class__.__name__.replace(
                "Processor",
                " Processor",
            )

            print(
                f"{name}: total "
                f"{processor._total_processed} items processed, "
                f"remaining {len(processor._data)} on processor"
            )


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")
    print("\nInitialize Data Stream...")

    stream = DataStream()
    stream.print_processors_stats()

    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    print("\nRegistering Processors\n")
    stream.register_processor(numeric)
    stream.register_processor(text)
    stream.register_processor(log)

    first_batch: list[Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected",
            },
        ],
        42,
        ["Hi", "five"],
    ]

    print(f"Send first batch of data on stream: {first_batch}")
    stream.process_stream(first_batch)
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, CSVExportPlugin())
    stream.print_processors_stats()

    second_batch: list[Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {
                "log_level": "ERROR",
                "log_message": "500 server crash",
            },
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]

    print(f"Send another batch of data: {second_batch}")
    stream.process_stream(second_batch)
    stream.print_processors_stats()

    print("Send 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, JSONExportPlugin())
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
