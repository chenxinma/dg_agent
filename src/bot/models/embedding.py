from modelscope.models import Model
from modelscope.pipelines import pipeline
from modelscope.utils.constant import Tasks
from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

from bot.settings import settings

model_id = settings.get_setting("chromadb.embedding_model")

pipeline_se = pipeline(Tasks.sentence_embedding,
                       model=model_id,
                       sequence_length=512
                       )

class GTEEmbeddingFunction(EmbeddingFunction):
    def __call__(self, inputs: Documents) -> Embeddings:
        result = pipeline_se(input={"source_sentence":inputs})
        return result['text_embedding'].tolist() # pyright: ignore[reportIndexIssue]