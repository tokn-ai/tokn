# GLM-4.7 reasoning then tool call

Captured from a real GLM-4.7 chat-completions stream.

The model emits:
1. A multi-chunk reasoning_content stream (16 deltas)
2. A single tool_calls chunk (`update_plan`, full arguments in one delta)
3. An empty assistant content chunk
4. A finish chunk with `finish_reason="tool_calls"` and usage (incl. `reasoning_tokens`)
5. `[DONE]`

Exercises:
- reasoning_text lifecycle from `reasoning_content`
- close-reasoning-before-opening-function-call ordering
- function_call lifecycle with name/call_id from chat tool_call envelope
- usage propagation (chat → responses, including reasoning_tokens)
- terminal `response.completed` + `[DONE]`
