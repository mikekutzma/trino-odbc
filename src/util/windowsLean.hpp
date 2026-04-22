#pragma once

#ifdef _WIN32 // _WIN32 is defined for both 32-bit and 64-bit Windows systems
#define VC_EXTRALEAN
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#define NOGDI
#include <windows.h>
#else

// SAL annotations are MSVC-only; define them as no-ops.
#ifndef _In_
#define _In_
#define _Out_
#define _Inout_
#define _In_opt_
#define _Out_opt_
#define _Inout_opt_
#define _In_reads_(x)
#define _In_reads_opt_(x)
#define _In_reads_bytes_(x)
#define _In_reads_bytes_opt_(x)
#define _Out_writes_(x)
#define _Out_writes_opt_(x)
#define _Out_writes_bytes_(x)
#define _Out_writes_bytes_opt_(x)
#define _Inout_updates_opt_(x)
#define _Inout_updates_bytes_opt_(x)
#define _Inexpressible_(x)
#define _Success_(x)
#define _Return_type_success_(x)
#endif

#endif
