/*****************************************************************************

Copyright (c) 1996, 2020, Oracle and/or its affiliates. All Rights Reserved.
Copyright (c) 2021, Huawei Technologies Co., Ltd.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#ifndef UNDO_SPACES_SNAPSHOT_H
#define UNDO_SPACES_SNAPSHOT_H

#include <atomic>
#include <vector>
#include "trx0purge.h"

#ifdef GMOCK_FOUND
#include "gtest/gtest_prod.h"
#endif

namespace undo {

class Undo_spaces_snapshot {
 public:
  Undo_spaces_snapshot();
  Undo_spaces_snapshot(const Undo_spaces_snapshot &) = delete;
  Undo_spaces_snapshot &operator=(const Undo_spaces_snapshot &) = delete;
  Undo_spaces_snapshot(Undo_spaces_snapshot &&) = delete;
  Undo_spaces_snapshot &operator=(Undo_spaces_snapshot &&) = delete;
  ~Undo_spaces_snapshot();
  /* Reset the snapshot and tickets. The method should never be called when
   * block_until_tickets_returned is being executed. */
  void reset(const unsigned int max_tickets,
             const std::vector<Tablespace *> &tablespaces);
  /* Return the undo tablespaces size in the snapshot. */
  std::size_t get_target_undo_tablespaces_size() const;
  /* Return the unused tickets number. */
  unsigned int get_unused_tickets_number() const;
  /* Request a ticket. It will repeatedly call try_request_ticket until either
  it succeeds or unused_tickets_number equals 0. 
  Return true if it succeeds.
  Return false if there is no unused ticket. */
  bool request_ticket();
  /* Return a ticket requested earlier. */
  void return_ticket();
  /* Block the caller until all used tickets have been returned. */
  void block_until_tickets_returned();
  /* Return the max tickets number. */
  unsigned int get_max_tickets_number() const;
  /* Wrapper */
  bool is_active_no_latch_for_undo_space(size_t pos) const;
  /* Wrapper */
  ulint get_rsegs_size_for_undo_space(size_t pos) const;
  /* Wrapper */
  trx_rseg_t *get_active_for_undo_space(size_t pos, ulint rseg_slot) const;

 private:
  /* Return the tablespace pointer at pos, or nullptr if pos is out of bound. */
  Tablespace *at(size_t pos) const;

 private:
  std::vector<Tablespace *> m_tablespaces;  
  unsigned int m_max_tickets;
  std::atomic<unsigned int> m_unused_tickets;
  std::atomic<unsigned int> m_used_tickets;
  std::atomic<bool> m_is_depriving;

#ifdef FRIEND_TEST
  FRIEND_TEST(UndoSpacesSnapshotTest, DefaultConstructor);
  FRIEND_TEST(UndoSpacesSnapshotTest, UseAllTickets);
  FRIEND_TEST(UndoSpacesSnapshotTest, UseSomeTickets);
#endif
}; /* class Undo_spaces_snapshot */
} /* namespace undo */

#endif  // UNDO_SPACES_SNAPSHOT_H
