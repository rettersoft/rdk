import test from 'ava'
import sinon from 'sinon'
import _ from 'lodash'
import RDK, { OperationsOutput, OperationsInput } from '../index'

const rdk = new RDK()

test.beforeEach(() => {})

const fakeInvokeLambda = async (payload: OperationsInput): Promise<OperationsOutput> => {
  const res = {} as OperationsOutput
  
  for (const [key, value] of Object.entries(payload)) {
    if (!res[key]) res[key] = []

    value.forEach(() => {
      res[key].push({
        success: true,
        data: "dummy"
      })
    })
  }

  return res
}

test.serial('dataPipeline chunk consuming test', async (t) => {
  const pipeline = rdk.dataPipeline()
  
  for (let i = 0; i < 50; i++) {
    pipeline.writeToDatabase({
      partKey: 'partKey',
      sortKey: 'sortKey',
      data: {i}
    })
  }

  for (let i = 0; i < 100; i++) {
    pipeline.readDatabase({
      partKey: 'partKey',
      sortKey: 'sortKey',
    })
  }

  // test generation of chunks
  const chunks = pipeline.generateChunks(pipeline.payload)
  t.is(chunks.writeToDatabase?.length, 2)
  t.is(chunks.readDatabase?.length, 1)


  const payload: OperationsInput = pipeline.consumeChunk(chunks)
  // generated payload
  t.is(payload.writeToDatabase?.length, 25)
  t.is(payload.readDatabase?.length, 100)

  // are chunks consumed?
  t.is(chunks.writeToDatabase?.length, 1)
  t.is(chunks.readDatabase?.length, 0)


  const nextpayload: OperationsInput = pipeline.consumeChunk(chunks)
  // generated payload
  t.is(nextpayload.writeToDatabase?.length, 25)
  t.is(nextpayload.readDatabase?.length, undefined)

  // are chunks consumed?
  t.is(chunks.writeToDatabase?.length, 0)
  t.is(chunks.readDatabase?.length, 0)
})

test.serial('dataPipeline response combining test', async (t) => {
  const pipeline = rdk.dataPipeline()

  sinon.stub(pipeline, 'send').callsFake(async (): Promise<OperationsOutput> => {
    const allRes: OperationsOutput = {} as OperationsOutput 

    // payload now can hold many operations so lets split them into chucks
    const chunks = pipeline.generateChunks(pipeline.payload)   

    t.is(chunks.writeToDatabase?.length, 2)
    t.is(chunks.readDatabase?.length, 1)

    // do this until all chucks are prosessed
    while (Object.values(chunks).some((v) => v.length > 0)) {
        
        const payload: OperationsInput = pipeline.consumeChunk(chunks)

        // send the operations

        const curRes = await fakeInvokeLambda(payload)

        // combine the responses
        _.mergeWith(allRes, curRes, (val1: OperationsOutput[], val2: OperationsOutput[]) => val1 ? val1.concat(val2) : val2)
    } 

    pipeline.payload = {}

    t.is(allRes.writeToDatabase?.length, 50)
    t.is(allRes.readDatabase?.length, 100)

    return allRes
  })
  
  for (let i = 0; i < 50; i++) {
    pipeline.writeToDatabase({
      partKey: 'partKey',
      sortKey: 'sortKey',
      data: {i}
    })
  }

  for (let i = 0; i < 100; i++) {
    pipeline.readDatabase({
      partKey: 'partKey',
      sortKey: 'sortKey',
    })
  }

  pipeline.send()
  
  t.pass()
})

