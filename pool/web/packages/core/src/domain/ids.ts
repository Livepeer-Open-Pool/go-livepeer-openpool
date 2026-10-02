import { Schema } from "effect"

// Every identifier is branded, so one cannot be passed where another belongs.

export const RegionId = Schema.NonEmptyString.pipe(Schema.brand("RegionId"))
export type RegionId = typeof RegionId.Type

export const MemberId = Schema.NonEmptyString.pipe(Schema.brand("MemberId"))
export type MemberId = typeof MemberId.Type

export const MachineId = Schema.NonEmptyString.pipe(Schema.brand("MachineId"))
export type MachineId = typeof MachineId.Type

export const AssignmentId = Schema.NonEmptyString.pipe(Schema.brand("AssignmentId"))
export type AssignmentId = typeof AssignmentId.Type

export const AppId = Schema.NonEmptyString.pipe(Schema.brand("AppId"))
export type AppId = typeof AppId.Type

export const GpuId = Schema.NonEmptyString.pipe(Schema.brand("GpuId"))
export type GpuId = typeof GpuId.Type

export const ApplicationId = Schema.NonEmptyString.pipe(Schema.brand("ApplicationId"))
export type ApplicationId = typeof ApplicationId.Type

export const RunnerId = Schema.NonEmptyString.pipe(Schema.brand("RunnerId"))
export type RunnerId = typeof RunnerId.Type

export const BatchId = Schema.NonEmptyString.pipe(Schema.brand("BatchId"))
export type BatchId = typeof BatchId.Type

/** An Ethereum address, in whatever case the service sent it. */
export const Address = Schema.String.pipe(Schema.pattern(/^0x[0-9a-fA-F]{40}$/), Schema.brand("Address"))
export type Address = typeof Address.Type

/** A session token, valid in every region of the pool. */
export const Token = Schema.NonEmptyString.pipe(Schema.brand("Token"))
export type Token = typeof Token.Type

/** A whole number of Livepeer rounds, or the number of one. */
export const Round = Schema.Int.pipe(Schema.nonNegative(), Schema.brand("Round"))
export type Round = typeof Round.Type
