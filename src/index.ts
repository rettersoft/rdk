export interface KeyValue {
    [key: string]: any
}
export interface KeyValueString {
    [key: string]: string
}

export interface Configuration {
    stepLimit?: number
}

export interface OperationResponse {
    success: boolean
    data?: any
    error?: string
}

export interface CloudObjectResponse {
    statusCode: number
    body?: any
    headers?: KeyValueString
}

export interface Response extends CloudObjectResponse {
    isBase64Encoded?: boolean
}

export interface Request {
    httpMethod: string
    body?: any
    headers: KeyValueString
    queryStringParams: KeyValueString
}

export interface Context {
    requestId: string
    projectId: string
    action: string
    identity: string
    serviceId?: string
    headers?: KeyValue
    classId: string
    instanceId?: string
    methodName: string
    refererClassId?: string
    refererInstanceId?: string
    refererMethodName?: string
    refererUserId?: string
    refererIdentity?: string
    claims?: KeyValue
    isAnonymous?: boolean
    culture?: string
    platform?: string
    userId?: string
    sourceIP: string
    sessionId?: string
    clientOs?: string
    targetServiceIds?: string[]
    relatedUserId?: string
}

export interface State {
    public?: KeyValue
    private?: KeyValue
    user?: { [userId: string]: { [key: string]: any } }
    role?: { [identity: string]: { [key: string]: any } }
}

export interface Schedule {
    classId?: string
    instanceId?: string
    payload?: any
    method: string
    after: number
}

interface RbsActionResponse {
    errorCode: string
    serviceId: string
    status: number
    errors: string[]
    response: any
    durationInMilliseconds: number
    executionDurationInMilliseconds: number
    headers: KeyValueString
    isExtract: boolean
}

interface RbsAction {
    name: string
    data?: KeyValue
    targetServiceIds?: string[]
    headers?: {
        classId: string
        instanceId: string
    }
    response?: RbsActionResponse[]
}

interface GetGlobalMemory {
    key: string
}

export interface SetGlobalMemory extends GetGlobalMemory {
    value: any
}
export interface GetFromSortedSet {
    setName: string
    sortKey: string
}

export interface AddToSortedSet extends GetFromSortedSet {
    data: Record<string, unknown>
}
export interface QuerySortedSet {
    setName: string
    beginsWith?: string
    greaterOrEqual?: string
    lessOrEqual?: string
    reverse?: boolean
    nextToken?: string
    limit?: number
}

export interface GetFile {
    filename: string
}

export interface SetFile extends GetFile {
    body: string
}
export interface LookUpKey {
    key: {
        name: string
        value: string
    }
}

export interface GetInstance {
    httpMethod?: string
    queryStringParams?: KeyValueString
    headers?: KeyValueString
    body?: any
    classId: string
    instanceId?: string
    lookupKey?: {
        name: string
        value: string
    }
}
export interface MethodCall extends GetInstance {
    methodName: string
}

export interface InitResponse {
    state?: State
    config?: Configuration
    response?: Response
}

export interface ReadOnlyOperationsInput {
    rbsAction?: RbsAction[]
    getGlobalMemory?: GetGlobalMemory[]
    getFromSortedSet?: GetFromSortedSet[]
    querySortedSet?: QuerySortedSet[]
    getFile?: GetFile[]
    getLookUpKey?: LookUpKey[]
    methodCall?: MethodCall[]
    getInstance?: GetInstance[]
}

export interface OperationsInput extends ReadOnlyOperationsInput {
    setGlobalMemory?: SetGlobalMemory[]
    deleteGlobalMemory?: GetGlobalMemory[]
    addToSortedSet?: AddToSortedSet[]
    removeFromSortedSet?: GetFromSortedSet[]
    setFile?: SetFile[]
    setLookUpKey?: LookUpKey[]
    deleteLookUpKey?: LookUpKey[]
}

export interface ReadonlyOperationsOutput {
    rbsAction?: RbsActionResponse[]
    getGlobalMemory?: OperationResponse[]
    getFromSortedSet?: OperationResponse[]
    querySortedSet?: OperationResponse[]
    getFile?: OperationResponse[]
    getLookUpKey?: OperationResponse[]
    methodCall?: CloudObjectResponse[]
    getInstance?: CloudObjectResponse[]
}

export interface OperationsOutput extends ReadonlyOperationsOutput {
    setGlobalMemory?: OperationResponse[]
    addToSortedSet?: OperationResponse[]
    removeFromSortedSet?: OperationResponse[]
    setFile?: OperationResponse[]
    setLookUpKey?: OperationResponse[]
    deleteLookUpKey?: OperationResponse[]
}

export interface StepResponse {
    state?: State
    methodState?: State
    response?: Response
    nextFlowId?: string
}

export interface Data extends StepResponse {
    context: Context
    env: KeyValue
    config: Configuration
    version: number
    state: State
    methodState: State
    request?: Request
    response: Response
    schedule: Schedule[]
    nextFlowId?: string
}

import { Lambda } from 'aws-sdk'
const lambda = new Lambda()

async function invokeLambda(payload: OperationsInput): Promise<OperationsOutput> {
    const { OPERATIONS_LAMBDA, CLOUD_OBJECTS_TOKEN } = process.env
    return lambda
        .invoke({
            FunctionName: OPERATIONS_LAMBDA!,
            Payload: JSON.stringify({ data: payload, token: CLOUD_OBJECTS_TOKEN! }),
        })
        .promise()
        .then(({ FunctionError: e, Payload: response }) => {
            if (e) throw new Error(e)

            return JSON.parse(response as string) as OperationsOutput
        })
}

export default class CloudObjectsOperator {
    pipeline(): CloudObjectsPipeline {
        return new CloudObjectsPipeline()
    }

    private async sendSingleOperation(input: any, operationType: string) {
        return invokeLambda({ [operationType]: [input] }).then((r) => r[operationType]?.pop())
    }
    async rbsAction(input: RbsAction): Promise<RbsActionResponse | undefined> {
        return this.sendSingleOperation(input, this.rbsAction.name)
    }
    async methodCall(input: MethodCall): Promise<CloudObjectResponse | undefined> {
        return this.sendSingleOperation(input, this.methodCall.name)
    }
    async getInstance(input: GetInstance): Promise<CloudObjectResponse | undefined> {
        return this.sendSingleOperation(input, this.getInstance.name)
    }
    async getLookUpKey(input: LookUpKey): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.getLookUpKey.name)
    }
    async setLookUpKey(input: LookUpKey): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.setLookUpKey.name)
    }
    async deleteLookUpKey(input: LookUpKey): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.deleteLookUpKey.name)
    }
    async setGlobalMemory(input: SetGlobalMemory): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.setGlobalMemory.name)
    }
    async getGlobalMemory(input: GetGlobalMemory): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.getGlobalMemory.name)
    }
    async deleteGlobalMemory(input: GetGlobalMemory): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.deleteGlobalMemory.name)
    }
    async addToSortedSet(input: AddToSortedSet): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.addToSortedSet.name)
    }
    async getFromSortedSet(input: GetFromSortedSet): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.getFromSortedSet.name)
    }
    async removeFromSortedSet(input: GetFromSortedSet): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.removeFromSortedSet.name)
    }
    async querySortedSet(input: QuerySortedSet): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.querySortedSet.name)
    }
    async getFile(input: GetFile): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.getFile.name)
    }
    async setFile(input: SetFile): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.setFile.name)
    }
}

class CloudObjectsPipeline {
    private payload: OperationsInput = {}
    rbsAction(input: RbsAction): CloudObjectsPipeline {
        if (!this.payload.rbsAction) this.payload.rbsAction = []
        this.payload.rbsAction.push(input)
        return this
    }
    getLookUpKey(input: LookUpKey): CloudObjectsPipeline {
        if (!this.payload.getLookUpKey) this.payload.getLookUpKey = []
        this.payload.getLookUpKey.push(input)
        return this
    }

    setLookUpKey(input: LookUpKey): CloudObjectsPipeline {
        if (!this.payload.setLookUpKey) this.payload.setLookUpKey = []
        this.payload.setLookUpKey.push(input)
        return this
    }
    deleteLookUpKey(input: LookUpKey): CloudObjectsPipeline {
        if (!this.payload.deleteLookUpKey) this.payload.deleteLookUpKey = []
        this.payload.deleteLookUpKey.push(input)
        return this
    }

    getGlobalMemory(input: GetGlobalMemory): CloudObjectsPipeline {
        if (!this.payload.getGlobalMemory) this.payload.getGlobalMemory = []
        this.payload.getGlobalMemory.push(input)
        return this
    }
    deleteGlobalMemory(input: GetGlobalMemory): CloudObjectsPipeline {
        if (!this.payload.deleteGlobalMemory) this.payload.deleteGlobalMemory = []
        this.payload.deleteGlobalMemory.push(input)
        return this
    }

    addToSortedSet(input: AddToSortedSet): CloudObjectsPipeline {
        if (!this.payload.addToSortedSet) this.payload.addToSortedSet = []
        this.payload.addToSortedSet.push(input)
        return this
    }
    getFromSortedSet(input: GetFromSortedSet): CloudObjectsPipeline {
        if (!this.payload.getFromSortedSet) this.payload.getFromSortedSet = []
        this.payload.getFromSortedSet.push(input)
        return this
    }
    removeFromSortedSet(input: GetFromSortedSet): CloudObjectsPipeline {
        if (!this.payload.removeFromSortedSet) this.payload.removeFromSortedSet = []
        this.payload.removeFromSortedSet.push(input)
        return this
    }
    querySortedSet(input: QuerySortedSet): CloudObjectsPipeline {
        if (!this.payload.querySortedSet) this.payload.querySortedSet = []
        this.payload.querySortedSet.push(input)
        return this
    }
    setGlobalMemory(input: SetGlobalMemory): CloudObjectsPipeline {
        if (!this.payload.setGlobalMemory) this.payload.setGlobalMemory = []
        this.payload.setGlobalMemory.push(input)
        return this
    }

    getFile(input: GetFile): CloudObjectsPipeline {
        if (!this.payload.getFile) this.payload.getFile = []
        this.payload.getFile.push(input)
        return this
    }

    setFile(input: SetFile): CloudObjectsPipeline {
        if (!this.payload.setFile) this.payload.setFile = []
        this.payload.setFile.push(input)
        return this
    }
    methodCall(input: MethodCall): CloudObjectsPipeline {
        if (!this.payload.methodCall) this.payload.methodCall = []
        this.payload.methodCall.push(input)
        return this
    }
    getInstance(input: GetInstance): CloudObjectsPipeline {
        if (!this.payload.getInstance) this.payload.getInstance = []
        this.payload.getInstance.push(input)
        return this
    }

    async send(): Promise<OperationsOutput> {
        return invokeLambda(this.payload).then((r) => {
            this.payload = {}
            return r
        })
    }
}
