import axios from 'axios'
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
export interface GenerateCustomTokenResponse extends OperationResponse {
    data: {
        customToken: string
    }
}
export interface CloudObjectResponse<T = any> {
    statusCode: number
    body?: T
    headers?: KeyValueString
}

export interface Response<T = any> extends CloudObjectResponse<T> {
    isBase64Encoded?: boolean
}

export interface Request<T = any> {
    httpMethod: string
    body?: T
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
    pathParameters?: {
        path: string
        rule?: string
        params?: KeyValueString
    }
}

type UserState = { [userId: string]: { [key: string]: any } }
type RoleState = { [identity: string]: { [key: string]: any } }

export interface State<PUB = KeyValue, PRIV = KeyValue, USER = UserState, ROLE = RoleState> {
    public?: PUB
    private?: PRIV
    user?: USER
    role?: ROLE
}

export interface Schedule {
    classId?: string
    instanceId?: string
    payload?: any
    method: string
    after: number
}
export interface GetMemory {
    key: string
    scopeClassId?: string
}
export interface DeleteMemory {
    key: string
    scope?: boolean
}

export interface SetMemory {
    key: string
    value: any
    expireAt?: number
    scope?: boolean
}
export interface GetFromSortedSet {
    setName: string
    sortKey: string
    scopeClassId?: string
}
export interface RemoveFromSortedSet {
    setName: string
    sortKey: string
    scope?: boolean
}

export interface AddToSortedSet {
    setName: string
    sortKey: string
    data: Record<string, unknown>
    scope?: boolean
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
export interface DeployClass {
    classId: string
    force?: boolean
}
export type Architecture = 'arm64' | 'x86_64' | 'x86_64,arm64' | 'arm64,x86_64'
export type Runtime = 'nodejs12.x,nodejs14.x' | 'nodejs12.x' | 'nodejs14.x' | 'nodejs14.x,nodejs12.x'
export interface UpsertDependency {
    dependencyName: string
    zipFile: Buffer
    architectures?: Architecture
    runtimes?: Runtime
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

export interface InitResponse<O = any> {
    state?: State
    config?: Configuration
    response?: Response<O>
}
export interface GenerateCustomToken {
    userId: string
    identity: string
    claims?: KeyValue
}

export interface ReadOnlyOperationsInput {
    getMemory?: GetMemory[]
    getFromSortedSet?: GetFromSortedSet[]
    querySortedSet?: QuerySortedSet[]
    getFile?: GetFile[]
    getLookUpKey?: LookUpKey[]
    methodCall?: MethodCall[]
    getInstance?: GetInstance[]
    getState?: GetInstance[]
    generateCustomToken?: GenerateCustomToken[]
}

export interface OperationsInput extends ReadOnlyOperationsInput {
    setMemory?: SetMemory[]
    deleteMemory?: GetMemory[]
    addToSortedSet?: AddToSortedSet[]
    removeFromSortedSet?: GetFromSortedSet[]
    setFile?: SetFile[]
    deleteFile?: GetFile[]
    setLookUpKey?: LookUpKey[]
    deleteLookUpKey?: LookUpKey[]
    upsertDependency?: UpsertDependency[]
    deployClass?: DeployClass[]
}

export interface ReadonlyOperationsOutput {
    getMemory?: OperationResponse[]
    getFromSortedSet?: OperationResponse[]
    querySortedSet?: OperationResponse[]
    getFile?: OperationResponse[]
    getLookUpKey?: OperationResponse[]
    methodCall?: CloudObjectResponse[]
    getInstance?: CloudObjectResponse[]
    getState?: CloudObjectResponse[]
    generateCustomToken?: GenerateCustomTokenResponse[]
}

export interface OperationsOutput extends ReadonlyOperationsOutput {
    setMemory?: OperationResponse[]
    deleteMemory?: OperationResponse[]
    addToSortedSet?: OperationResponse[]
    removeFromSortedSet?: OperationResponse[]
    setFile?: OperationResponse[]
    deleteFile?: OperationResponse[]
    setLookUpKey?: OperationResponse[]
    deleteLookUpKey?: OperationResponse[]
    upsertDependency?: OperationResponse[]
    deployClass?: OperationResponse[]
}

export interface StepResponse<T = any, PUB = KeyValue, PRIV = KeyValue, USER = UserState, ROLE = RoleState> {
    state?: State<PUB, PRIV, USER, ROLE>
    methodState?: State<PUB, PRIV, USER, ROLE>
    response?: Response<T>
    nextFlowId?: string
}

export interface Data<I = any, O = any, PUB = KeyValue, PRIV = KeyValue, USER = UserState, ROLE = RoleState>
    extends StepResponse<O, PUB, PRIV, USER, ROLE> {
    context: Context
    env: KeyValue
    config: Configuration
    version: number
    state: State<PUB, PRIV, USER, ROLE>
    methodState: State<PUB, PRIV, USER, ROLE>
    request: Request<I>
    response: Response<O>
    schedule: Schedule[]
    nextFlowId?: string
}

import { Lambda } from 'aws-sdk'
let lambda: Lambda | undefined = undefined
let token = ''
let operationLambda = ''
export function init(c: any, t: string, o: string) {
    lambda = new Lambda({ credentials: c })
    token = t
    operationLambda = o
}

async function invokeLambda(payload: OperationsInput): Promise<OperationsOutput> {
    return lambda!
        .invoke({
            FunctionName: operationLambda,
            Payload: JSON.stringify({ data: payload, token }),
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
    async generateCustomToken(input: GenerateCustomToken): Promise<GenerateCustomTokenResponse | undefined> {
        return this.sendSingleOperation(input, this.generateCustomToken.name)
    }
    async methodCall(input: MethodCall): Promise<CloudObjectResponse | undefined> {
        return this.sendSingleOperation(input, this.methodCall.name)
    }
    async getInstance(input: GetInstance): Promise<CloudObjectResponse | undefined> {
        return this.sendSingleOperation(input, this.getInstance.name)
    }
    async getState(input: GetInstance): Promise<CloudObjectResponse | undefined> {
        return this.sendSingleOperation(input, this.getState.name)
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
    async setMemory(input: SetMemory): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.setMemory.name)
    }
    async getMemory(input: GetMemory): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.getMemory.name)
    }
    async deleteMemory(input: DeleteMemory): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.deleteMemory.name)
    }
    async addToSortedSet(input: AddToSortedSet): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.addToSortedSet.name)
    }
    async getFromSortedSet(input: GetFromSortedSet): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.getFromSortedSet.name)
    }
    async removeFromSortedSet(input: RemoveFromSortedSet): Promise<OperationResponse | undefined> {
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
    async deleteFile(input: GetFile): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.deleteFile.name)
    }
    async deployClass(input: DeployClass): Promise<OperationResponse | undefined> {
        return this.sendSingleOperation(input, this.deployClass.name)
    }
    async upsertDependency(input: UpsertDependency): Promise<OperationResponse | undefined> {
        const limit = 50000000
        return this.sendSingleOperation({ ...input, commit: false, zipFile: undefined }, this.upsertDependency.name).then((r: OperationResponse) => {
            if (!r.success) return r
            return axios
                .put(r.data.url, input.zipFile, {
                    headers: {
                        'Content-Type': 'application/zip',
                    },
                    maxBodyLength: limit,
                    maxContentLength: limit,
                })
                .then(() =>
                    this.sendSingleOperation({ ...input, commit: true, zipFile: undefined }, this.upsertDependency.name).catch(
                        (e) => ({ success: false, error: e.message } as OperationResponse),
                    ),
                )
        })
    }
}

export class CloudObjectsPipeline {
    private payload: OperationsInput = {}
    generateCustomToken(input: GenerateCustomToken): CloudObjectsPipeline {
        if (!this.payload.generateCustomToken) this.payload.generateCustomToken = []
        this.payload.generateCustomToken.push(input)
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
    setMemory(input: SetMemory): CloudObjectsPipeline {
        if (!this.payload.setMemory) this.payload.setMemory = []
        this.payload.setMemory.push(input)
        return this
    }
    getMemory(input: GetMemory): CloudObjectsPipeline {
        if (!this.payload.getMemory) this.payload.getMemory = []
        this.payload.getMemory.push(input)
        return this
    }
    deleteMemory(input: DeleteMemory): CloudObjectsPipeline {
        if (!this.payload.deleteMemory) this.payload.deleteMemory = []
        this.payload.deleteMemory.push(input)
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
    removeFromSortedSet(input: RemoveFromSortedSet): CloudObjectsPipeline {
        if (!this.payload.removeFromSortedSet) this.payload.removeFromSortedSet = []
        this.payload.removeFromSortedSet.push(input)
        return this
    }
    querySortedSet(input: QuerySortedSet): CloudObjectsPipeline {
        if (!this.payload.querySortedSet) this.payload.querySortedSet = []
        this.payload.querySortedSet.push(input)
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

    deleteFile(input: GetFile): CloudObjectsPipeline {
        if (!this.payload.deleteFile) this.payload.deleteFile = []
        this.payload.deleteFile.push(input)
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
    getState(input: GetInstance): CloudObjectsPipeline {
        if (!this.payload.getState) this.payload.getState = []
        this.payload.getState.push(input)
        return this
    }
    deployClass(input: DeployClass): CloudObjectsPipeline {
        if (!this.payload.deployClass) this.payload.deployClass = []
        this.payload.deployClass.push(input)
        return this
    }

    async send(): Promise<OperationsOutput> {
        return invokeLambda(this.payload).then((r) => {
            this.payload = {}
            return r
        })
    }
}
