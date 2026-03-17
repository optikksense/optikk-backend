package runs

type llmRunRowDTO LLMRun
type llmRunSummaryRowDTO LLMRunSummary
type llmRunModelRowDTO LLMRunModel
type llmRunOperationRowDTO LLMRunOperation

func mapLLMRunDTOs(rows []llmRunRowDTO) []LLMRun {
	out := make([]LLMRun, len(rows))
	for i, row := range rows {
		out[i] = LLMRun(row)
	}
	return out
}

func mapLLMRunModelDTOs(rows []llmRunModelRowDTO) []LLMRunModel {
	out := make([]LLMRunModel, len(rows))
	for i, row := range rows {
		out[i] = LLMRunModel(row)
	}
	return out
}

func mapLLMRunOperationDTOs(rows []llmRunOperationRowDTO) []LLMRunOperation {
	out := make([]LLMRunOperation, len(rows))
	for i, row := range rows {
		out[i] = LLMRunOperation(row)
	}
	return out
}
